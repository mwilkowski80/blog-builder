from typing import Dict, Any
import requests
import signal
from duckduckgo_search import DDGS
from googlesearch import search as google_search
from retry import retry
import os
from structlog.stdlib import get_logger as get_raw_logger
import re
from itertools import chain, product

from s8er.llm import CachedOpenAI


logger = get_raw_logger(os.path.basename(__file__))

CHAT_TRUE_VALUES = [True, "True", "true"]
CHAT_FALSE_VALUES = [False, "False", "false"]
CHAT_ONE_VALUES = [1, "1", "One", "one"]
CHAT_UNKNOWN_STR_LOWER_VALUES = ["none", "null", "", "unknown", "n/a", "unk", "not available", "unnamed"]

POSITION_HELD_GROUPS = {
    # Standard processor
    "Executive branch of government",
    "Central banking and financial integrity",
    "Head of government or state",
    "Judicial branch of government",
    "Legislative branch of government",
    "National government",
    "Security services",

    # State government processor
    "State government",

    # Major Cities processor
    "City",

    # TODO
    "Municipal government",
    "State-owned enterprise",
    "Intergovernmental organization",
    "Military",
    "Diplomat",
}


WEB_CONTENT_MAX_LEN = 12000


def ask_chat(
    cached_response_client: CachedOpenAI,
    input_str: str,
    input_kwargs: dict = None,
) -> Dict[str, Any]:
    if not input_kwargs:
        input_kwargs = {}

    logger.info("Running LLM query...")
    chat_completion = cached_response_client.query(
        input_str=input_str,
        **input_kwargs,
    )
    return chat_completion


def get_url(url: str) -> str:
    r = requests.get(url)
    r.raise_for_status()
    return r.text


def is_answer_available(chat_response: Dict[str, Any]) -> bool:
    return (chat_response.get("answer-available") in CHAT_TRUE_VALUES) \
        or (chat_response.get("answer_available") in CHAT_TRUE_VALUES)


class timeout:
    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


def get_ddg_search(
    max_results: int,
    num_tries: int = 3,
    retry_delay: int = 0,
    backoff: int = 1,
    modes=("text",),
    regions=("wt-wt",),
):
    assert all([mode in ["text", "news"] for mode in modes])

    @retry(tries=num_tries, delay=retry_delay, backoff=backoff, logger=logger)
    def ddg_search(search_str):
        web_search_results = []
        for mode, region in product(modes, regions):
            with DDGS() as ddgs:
                match mode:
                    case "text":
                        web_search_results.extend([r for r in ddgs.text(search_str, max_results=max_results, region=region)])
                    case "news":
                        web_search_results.extend([r for r in ddgs.news(search_str, max_results=max_results, region=region)])
        return web_search_results
    
    return ddg_search


def get_google_search(
    max_results: int,
    num_tries: int = 3,
    retry_delay: int = 0,
    backoff: int = 1,
    # modes=("text",),
    languages=("en",),
):
    @retry(tries=num_tries, delay=retry_delay, backoff=backoff, logger=logger)
    def google_search_func(search_str):
        web_search_results = []
        for lang in languages:
            web_search_results.extend([r for r in google_search(search_str, num_results=max_results, advanced=True, lang=lang)])
        
        web_search_results = [
            {
                "title": result.title,
                "href": result.url,
                "body": result.description,
            } for result in web_search_results
        ]
        return web_search_results
    return google_search_func


def answer_to_bool(answer: str, default: bool = None):
    if bool(re.match(r"^[Yy]es", answer)):
        return True

    elif bool(re.match(r"^[Nn]o", answer)):
        return False

    return default


def validate_name_partial(name_partial: str) -> bool:
    return bool(re.match("^[\w'\-,.][^0-9_!¡?÷?¿/\\+=@#$%ˆ&*(){}|~<>;:[\]]{1,}$", name_partial))


def validate_name(name):
    name_partials = name.split()
    return len(name_partials) > 1 and all([validate_name_partial(name_partial) for name_partial in name_partials])


def merge_dicts(*dicts: dict, skipkeys: list = None) -> dict:
    all_keys = set(chain.from_iterable(_dict.keys() for _dict in dicts))

    ret_dict = {}
    for key in all_keys:
        try:
            source_properties = [_dict.get(key) for _dict in dicts]

            non_none_properties = [prop for prop in source_properties if prop is not None]
            properties_types = set([type(prop) for prop in non_none_properties])
            assert len(properties_types) <= 1

            if len(non_none_properties) == 0:
                ret_dict[key] = None

            elif isinstance(non_none_properties[0], list):
                ret_dict[key] = list(chain.from_iterable(non_none_properties))

            elif isinstance(non_none_properties[0], dict):
                if skipkeys and key in skipkeys:
                    ret_dict[key] = source_properties
                else:
                    ret_dict[key] = merge_dicts(*non_none_properties, skipkeys=skipkeys)

            else:
                ret_dict[key] = non_none_properties
        except Exception as exc:
            logger.error(
                "Merging proprties failed",
                exc=exc.__class__.__name__,
                msg=str(exc),
                source_properties=source_properties,
            )

    return ret_dict
