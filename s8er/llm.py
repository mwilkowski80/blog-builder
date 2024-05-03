import hashlib
import json
import os
from timeit import default_timer as timer
from structlog.stdlib import get_logger as get_raw_logger

from openai import OpenAI

from s8er.cache import Cache


logger = get_raw_logger(os.path.basename(__file__))

CHAT_3_5_TURBO_INPUT_COST = 0.5 / 10e6
CHAT_3_5_TURBO_OUTPUT_COST = 1.5 / 10e6


class CachedOpenAI:
    def __init__(self, cache: Cache, openai_args: dict, model_name: str):
        self._cache = cache
        self._openai_args = openai_args
        self._model_name = model_name
        args_text = json.dumps({'openai-args': openai_args, 'model-name': model_name})
        args_hash = hashlib.md5(args_text.encode('utf-8')).hexdigest()
        self._prefix_key = 'OPENAI-' + args_hash + '-'

    def query(self, input_str: str, **kwargs) -> str:
        return self._cache.get(
            key=input_str,
            prefix_key=self._prefix_key,
            supplier=lambda: self._query(input_str, **kwargs)
        ).payload

    def _query(self, input_str: str, **kwargs) -> str:
        client = OpenAI(**self._openai_args)

        start = timer()
        chat_completion = client.chat.completions.create(
            messages=[{"role": "user", "content": input_str}],
            model=self._model_name,
            **kwargs
        )
        end = timer()

        prompt_cost = chat_completion.usage.prompt_tokens * CHAT_3_5_TURBO_INPUT_COST + \
            chat_completion.usage.completion_tokens * CHAT_3_5_TURBO_OUTPUT_COST

        logger.info(
            "Prompt details",
            num_input_tokens=chat_completion.usage.prompt_tokens,
            num_output_tokens=chat_completion.usage.completion_tokens,
            chat_response_time=f"{end-start:.2f}s",
            prompt_cost=f"{prompt_cost:.6f}$"
        )
        return chat_completion.choices[0].message.content.strip()
