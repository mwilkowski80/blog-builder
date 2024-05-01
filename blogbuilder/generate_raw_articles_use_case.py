import json
import logging
import re
import traceback
from hashlib import md5
from pathlib import Path
from typing import List, Callable

import backoff as backoff
import requests
from tqdm import tqdm

from blogbuilder.llm import LLM


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
    def __init__(self, llm: LLM, persist_summary: PersistSummary,
                 websearch_func: Callable[[str], List[str]],
                 download_timeout: int) -> None:
        self._llm = llm
        self._persist_summary = persist_summary
        self._websearch_func = websearch_func
        self._log = logging.getLogger(__package__ + '.' + GenerateRawArticlesUseCase.__name__)
        self._download_timeout = download_timeout

    def _get_queries_from_llm(self) -> List[str]:
        queries_str = self._llm("""
        I want to create a blog about financial crime and compliance. I consider this topic elementary and very important for wide public. People do not know what financial crime usually is and do not understand the procedures that banks have to undertake to deal with the problem. I want to fill this gap and allow regular people read and understand the topic of financial crime.

        I want to build this blog based on the input from the Internet and analyze the content available in the Internet already. I want to use DuckDuckGo search engine to look for interesting articles. Please generate an exhaustive list of search queries that I should invoke in DuckDuckGo to find articles that I am interested in.

        Please return the results in the following JSON format:
        {"queries": ["query 1", "query 2", "query 3"]} and so on.

        I want to consume the output automatically, therefore please do not return anything else.
                """)
        return json.loads(queries_str)['queries']

    def _get_queries_from_text(self) -> List[str]:
        return [
            "what is financial crime", "types of financial crime", "examples of financial crime",
            "financial crime prevention", "financial crime compliance", "anti-money laundering", "AML procedures",
            "know your customer KYC", "suspicious activity reports", "financial crime investigations",
            "financial crime regulations", "financial crime laws", "financial crime enforcement",
            "financial crime penalties", "financial crime statistics", "financial crime trends",
            "financial crime case studies", "financial crime news", "financial crime blogs", "financial crime podcasts",
            "financial crime books", "financial crime documentaries", "financial crime awareness",
            "financial crime education", "financial crime training", "financial crime jobs", "financial crime careers",
            "financial crime technology", "financial crime software", "financial crime tools",
            "financial crime risk assessment", "financial crime risk management", "financial crime compliance programs",
            "financial crime compliance best practices", "financial crime compliance challenges",
            "financial crime compliance costs", "financial crime compliance trends", "financial crime compliance news",
            "financial crime compliance blogs", "financial crime compliance podcasts",
            "financial crime compliance books", "financial crime compliance certifications",
            "financial crime compliance jobs", "financial crime compliance careers",
            "financial institutions and crime prevention", "banks and financial crime",
            "role of banks in preventing financial crime", "how banks detect financial crime", "bank secrecy act",
            "USA PATRIOT Act", "FATF recommendations", "Basel AML Index", "FinCEN", "OFAC sanctions",
            "financial crime in the cryptocurrency industry", "money laundering through real estate",
            "trade-based money laundering", "financial crime and terrorist financing",
            "financial crime and organized crime", "financial crime and corruption", "financial crime and tax evasion",
            "financial crime and fraud", "financial crime and cybercrime", "financial crime and virtual currencies",
            "financial crime and offshore banking", "financial crime and shell companies",
            "financial crime and politically exposed persons (PEPs)", "financial crime and beneficial ownership",
            "financial crime and customer due diligence (CDD)", "financial crime and enhanced due diligence (EDD)",
            "financial crime and risk-based approach", "financial crime and transaction monitoring",
            "financial crime and sanctions screening", "financial crime and adverse media screening",
            "financial crime and red flags", "financial crime and typologies", "financial crime and emerging threats",
            "financial crime and regulatory compliance", "financial crime and corporate governance",
            "financial crime and ethics", "financial crime and social responsibility",
            "impact of financial crime on society", "cost of financial crime", "fighting financial crime",
            "preventing financial crime", "detecting financial crime", "reporting financial crime",
            "investigating financial crime", "prosecuting financial crime"]

    def _get_queries_from_text_2(self) -> List[str]:
        return [
            "what is financial crime", "types of financial crime", "examples of financial crime",
            "financial crime prevention", "financial crime compliance", "anti-money laundering", "AML procedures",
            "know your customer KYC", "suspicious activity reports", "financial crime investigations"]

    def _get_queries_from_text_3(self) -> List[str]:
        return [
            "financial crime statistics", "financial crime trends",
            "financial crime case studies", "financial crime news", "financial crime blogs", "financial crime podcasts",
            "financial crime books", "financial crime documentaries", "financial crime awareness",
            "financial crime education", "financial crime training", "financial crime jobs", "financial crime careers",
            "financial crime technology", "financial crime software", "financial crime tools",
            "financial crime risk assessment", "financial crime risk management", "financial crime compliance programs",
            "financial crime compliance best practices", "financial crime compliance challenges",
            "financial crime compliance costs", "financial crime compliance trends", "financial crime compliance news",
            "financial crime compliance blogs", "financial crime compliance podcasts",
            "financial crime compliance books", "financial crime compliance certifications",
            "financial crime compliance jobs", "financial crime compliance careers",
            "financial institutions and crime prevention", "banks and financial crime",
            "role of banks in preventing financial crime", "how banks detect financial crime", "bank secrecy act",
            "USA PATRIOT Act", "FATF recommendations", "Basel AML Index", "FinCEN", "OFAC sanctions",
            "financial crime in the cryptocurrency industry", "money laundering through real estate",
            "trade-based money laundering", "financial crime and terrorist financing",
            "financial crime and organized crime", "financial crime and corruption", "financial crime and tax evasion",
            "financial crime and fraud", "financial crime and cybercrime", "financial crime and virtual currencies",
            "financial crime and offshore banking", "financial crime and shell companies",
            "financial crime and politically exposed persons (PEPs)", "financial crime and beneficial ownership",
            "financial crime and customer due diligence (CDD)", "financial crime and enhanced due diligence (EDD)",
            "financial crime and risk-based approach", "financial crime and transaction monitoring",
            "financial crime and sanctions screening", "financial crime and adverse media screening",
            "financial crime and red flags", "financial crime and typologies", "financial crime and emerging threats",
            "financial crime and regulatory compliance", "financial crime and corporate governance",
            "financial crime and ethics", "financial crime and social responsibility",
            "impact of financial crime on society", "cost of financial crime", "fighting financial crime",
            "preventing financial crime", "detecting financial crime", "reporting financial crime",
            "investigating financial crime", "prosecuting financial crime"]

    def invoke(self) -> None:
        queries = self._get_queries_from_text()
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
            self._log.info(f'Checking if the page is related to the phrase: {query}')
            llm_query = f"""
I want to create a blog about financial crime compliance. Here is a topic that I want to write about: {query}.

Please check the content of the page below and evaluate how much this topic is related to the page content. Please answer using only a single integer number in scale 0-100 where 0 means completely unrelated and 100 means fully related. Do not output anything else but this number as I want to process the output automatically.

Example output 1:
0

Example output 2:
30

Example output 3:
100

Example output 4:
73

Here is the content of the page:
{page_html[:12000]}"""
            output = self._llm(llm_query)
            return _extract_int_from_llm_output(output) > 50

        return _inner_check()

    def _summarize_the_page_for_me(self, page_html: str, topic: str) -> str:
        self._log.info(f'Summarizing the page for the topic: {topic}')
        llm_query = f"""
Please rewrite the following webpage in a way that it looks like a media article about the following topic: "{topic}". Generate just the article text without formatting. Here is the webpage HTML content that you should rewrite:
 
f{page_html[:12000]}"""
        return self._llm(llm_query)

    def _capture_summary(self, query: str, url: str, summary: str) -> None:
        self._log.info(f'Capturing the summary for the query: {query}')
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
            self._log.info(f'Downloading URL: {url}')
            r = requests.get(url, timeout=self._download_timeout)
            r.raise_for_status()
            page_html = r.text
            if self._check_if_page_is_related_to_phrase(page_html, query):
                summary = self._summarize_the_page_for_me(page_html, query)
                self._capture_summary(query=query, url=url, summary=summary)

        _inner_process_url()
