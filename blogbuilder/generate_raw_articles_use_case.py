import json
import logging
import os
import re
import traceback
from hashlib import md5
from pathlib import Path
from subprocess import check_output
from typing import List, Callable

import backoff as backoff
import requests
from tqdm import tqdm

from blogbuilder.llm import LLM
from s8er.cache import Cache


def _extract_int_from_llm_output(llm_output: str) -> int:
    if not llm_output:
        raise ValueError('LLM output was empty')

    lines = llm_output.strip().splitlines(False)
    if len(lines) == 0:
        raise ValueError('LLM output was blank (whitespace characters only)')

    return int(lines[0].strip().split('.')[0])


class PersistSummary:
    def persist(self, query: str, url: str, summary: str) -> None:
        raise NotImplementedError()

    def exists(self, query: str, url: str) -> bool:
        raise NotImplementedError()


def _sanitize_filename(s: str) -> str:
    sanitized = re.sub(r'\s+', '_', s)
    sanitized = re.sub(r'[<>:"/\\|?*]', '', sanitized)
    return sanitized


class PersistSummaryToFile(PersistSummary):
    def __init__(self, output_dir: str) -> None:
        self._output_dir = output_dir

    def persist(self, query: str, url: str, summary: str) -> None:
        with open(self._output_filepath(query, url), 'w') as f:
            f.write(summary)

    def exists(self, query: str, url: str) -> bool:
        return self._output_filepath(query, url).exists()

    def _output_filepath(self, query: str, url: str) -> Path:
        return Path(self._output_dir) / _sanitize_filename(
            query + '-' + url[:50] + md5(url.encode('utf-8')).hexdigest())


class GenerateRawArticlesUseCase:
    def __init__(self, topic_generator_func: Callable[[], List[str]],
                 llm: LLM, persist_summary: PersistSummary,
                 websearch_func: Callable[[str], List[str]],
                 download_timeout: int,
                 check_cache: Cache[bool],
                 max_llm_payload: int,
                 ) -> None:
        self._topic_generator_func = topic_generator_func
        self._llm = llm
        self._persist_summary = persist_summary
        self._websearch_func = websearch_func
        self._log = logging.getLogger(__package__ + '.' + GenerateRawArticlesUseCase.__name__)
        self._download_timeout = download_timeout
        self._check_cache = check_cache
        self._max_llm_payload = max_llm_payload

    def invoke(self) -> None:
        queries = self._topic_generator_func()
        for query in queries:
            try:
                urls = self._websearch_func(query)
                for url in tqdm(urls):
                    self._process_url(query, url)
            except:
                traceback.print_exc()

    def _check_if_page_is_related_to_phrase(self, page_html: str, query: str) -> bool:
        def _backoff_handler(details: dict):
            self._log.warning(f'Backing off: {query}')

        def _giveup_handler(details: dict):
            self._log.error(f'Giving up: {query}')

        @backoff.on_exception(backoff.expo, Exception, max_tries=3,
                              on_backoff=_backoff_handler, on_giveup=_giveup_handler)
        def _inner_check() -> bool:
            self._log.info(f'Checking if the page (len: {len(page_html)}) is related to the phrase: {query}')
            llm_query = self._generate_prompt_to_check_if_content_is_related(query, page_html)
            output = self._llm(llm_query).strip().upper().replace('\\', '').replace('_', '').replace(' ', '')
            if not output.startswith('CANNOTPROCESS') and \
                    not output.startswith('UNRELATED') and \
                    not output.startswith('SOMEWHATRELATED') and \
                    not output.startswith('STRONGLYRELATED') and \
                    not output.startswith('FULLYRELATED'):
                raise ValueError(f'Unexpected output: "{output}"')
            return output.startswith('STRONGLYRELATED') or output.startswith('FULLYRELATED')

        return _inner_check()

    def _generate_prompt_to_check_if_content_is_related(self, topic: str, content: str) -> str:
        return f"""
I want to create a blog about financial crime compliance. Here is a topic that I want to write about: {topic}.

Please check the content below and evaluate how much this topic is related to the page content. I am looking for one of the answers:
- CANNOT_PROCESS - you cannot process the text, i.e. it is unreadable or encoded in unreadable format
- UNRELATED - text is very unrelated to the topic
- SOMEWHAT_RELATED - text is somewhat related to the topic
- STRONGLY_RELATED - text is strongly related to the topic
- FULLY_RELATED - text is fully related to the topic

Please answer only with one of the above terms:
CANNOT_PROCESS
UNRELATED
SOMEWHAT_RELATED
STRONGLY_RELATED
FULLY_RELATED

Do not output anything else but one of the above terms as I want to process the output automatically.

Example output 1:
UNREADABLE

Example output 2:
UNRELATED

Example output 3:
SOMEWHAT_RELATED

Example output 4:
STRONGLY_RELATED

Example output 5:
FULLY_RELATED

Here is the content of the page:
{content[:self._max_llm_payload]}"""

    def _summarize_the_page_for_me(self, page_html: str, topic: str) -> str:
        self._log.info(f'Summarizing the page. Length: {len(page_html)}. Topic: {topic}')
        llm_query = f"""
Please rewrite the following webpage in a way that it looks like a media article about the following topic: "{topic}". Generate just the article text without formatting. Here is the webpage HTML content that you should rewrite:
 
f{page_html[:self._max_llm_payload]}"""
        return self._llm(llm_query)

    def _persist_summary(self, query: str, url: str, summary: str) -> None:
        self._log.info(f'Persisting the summary for the query: {query}')
        self._persist_summary.persist(query=query, url=url, summary=summary)

    def _process_url(self, query: str, url: str) -> None:
        if self._persist_summary.exists(query=query, url=url):
            self._log.info(f'Skipping URL-query: {url}-{query}')
        else:
            self._do_process_url(query, url)

    def _do_process_url(self, query: str, url: str) -> None:
        def _backoff_handler(details: dict):
            self._log.warning(f'Backing off: {url}')

        def _giveup_handler(details: dict):
            self._log.error(f'Giving up: {url}')

        @backoff.on_exception(
            backoff.expo, Exception,
            on_backoff=_backoff_handler,
            on_giveup=_giveup_handler,
            max_tries=3)
        def _inner_process_url() -> None:
            check_cache_key = f'{query}-{url}'
            if not self._check_cache.exists(check_cache_key):
                self._log.info(f'About to obtain content for topic: {query} from URL: {url}')
                page_content = self._obtain_content_from_url(url)
                if page_content and page_content.strip() and self._check_cache.get_raw(
                        check_cache_key,
                        lambda: self._check_if_page_is_related_to_phrase(page_content, query),
                        'CHECK-'):
                    summary = self._summarize_the_page_for_me(page_content, query)
                    self._persist_summary.persist(query=query, url=url, summary=summary)
                else:
                    self._log.info(f'Skipping URL-query: {url}-{query}')
            else:
                self._log.info(f'Skipping URL-query (based on check cache): {url}-{query}')

        _inner_process_url()

    def _obtain_content_from_url(self, url: str) -> str:
        r = requests.get(url, timeout=self._download_timeout)
        r.raise_for_status()
        page_content = r.text
        return page_content


class GenerateRawArticles2UseCase(GenerateRawArticlesUseCase):
    def __init__(self, obtain_content_func: Callable[[str], str], **kwargs):
        super().__init__(**kwargs)
        self._obtain_content_func = obtain_content_func

    def _obtain_content_from_url(self, url: str) -> str:
        return self._obtain_content_func(url)

    def _summarize_the_page_for_me(self, page_html: str, topic: str) -> str:
        self._log.info(f'Summarizing the page. Length: {len(page_html)}. Topic: {topic}')
        llm_query = f"""
Please rewrite the following article in a way that it looks like a media article about the following topic: "{topic}". Generate just the article text without formatting. Here is the article content that you should rewrite:

f{page_html[:self._max_llm_payload]}"""
        return self._llm(llm_query)

    def _generate_prompt_to_check_if_content_is_related(self, topic: str, content: str) -> str:
        return f"""
Here is the content of the page:
===
{content[:self._max_llm_payload]}
===

I want to create a blog about financial crime compliance.

Please check the content of the page above and evaluate how much it is related to the topic: {topic}. I am looking for one of the answers:
- CANNOT_PROCESS - you cannot process the text, i.e. it is unreadable or encoded in unreadable format
- UNRELATED - text is very unrelated to the topic
- SOMEWHAT_RELATED - text is somewhat related to the topic
- STRONGLY_RELATED - text is strongly related to the topic
- FULLY_RELATED - text is fully related to the topic

Please answer only with one of the above terms:
CANNOT_PROCESS
UNRELATED
SOMEWHAT_RELATED
STRONGLY_RELATED
FULLY_RELATED

Do not output anything else but one of the above terms as I want to process the output automatically.

Example output 1:
UNREADABLE

Example output 2:
UNRELATED

Example output 3:
SOMEWHAT_RELATED

Example output 4:
STRONGLY_RELATED

Example output 5:
FULLY_RELATED
"""
