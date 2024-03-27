import logging
import os.path
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Iterable

import click
import googlesearch
import requests
from bs4 import BeautifulSoup
from openai import OpenAI

from s8er.cache import FilesystemCache
from s8er.llm import CachedOpenAI
from s8pwa.util.logging import basic_logging_config

log: Optional[logging.Logger]


@dataclass
class QueryResponseEntry:
    url: str
    title: str


@dataclass
class QueryResponse:
    abstract: str
    entries: List[QueryResponseEntry]


def query_google_raw(input_str: str) -> str:
    input_str_encoded = urllib.parse.quote_plus(input_str)
    r = requests.get(f'https://google.com/search?={input_str_encoded}')
    r.raise_for_status()
    return r.text


def get_url(url: str) -> str:
    r = requests.get(url)
    r.raise_for_status()
    return r.text


def query_1() -> str:
    output = get_url('https://governo.gov.ao/provincia/bengo')
    soup = bs4.BeautifulSoup(output)
    content = soup.text
    vicuna_output = ask_vicuna(f"""
    Please analyze the content of the page and list who were mentioned as current governors of the province Bengo in country Angola.
    
    Answer in the following format:
    {{"answer-available": <true_or_false>, "answer": [{{"position": "<position_here">, "name": "<name_here>"}}]}}
    
    Content of web page:
    {content[:12000]}
    """.strip())
    pass


def query_2() -> str:
    output = get_url('https://governo.gov.ao/provincia/bengo')
    soup = BeautifulSoup(output)
    content = soup.text
    vicuna_output = ask_vicuna(f"""
    Please analyze the content of the page and list who were mentioned as current governors of the province Bengo in country Angola.

    Answer in the following format:
    {{"answer-available": <true_or_false>, "answer": [{{"position": "<position_here">, "name": "<name_here>"}}]}}

    Content of web page:
    {content[:12000]}
    """.strip())
    pass


def query_search_engine(search_text: str) -> Iterable[dict]:
    for sr in googlesearch.search(search_text, advanced=True):
        yield {'url': sr.url, 'title': sr.title, 'description': sr.description}


def _extract_article_text(raw_html: str) -> str:
    return BeautifulSoup(raw_html, features='html.parser').text


def _llm_analyze_page_if_role_mentioned(openai: CachedOpenAI, role: str, content: str) -> str:
    vicuna_output = openai.query(f"""
    Please analyze the content of the page and list who were mentioned as {role}.

    Answer in the following format:
    {{"answer-available": <true_or_false>, "list-of-people": [{{"person-position": "<person_position_here">, "person-name": "<person_name_here>"}}]}}

    Content of web page:
    {content[:12000]}
    """.strip())
    return vicuna_output


def _llm_analyze_snippet_if_role_mentioned(openai: CachedOpenAI, role: str, content: str) -> str:
    vicuna_output = openai.query(f"""
    Please analyze the short search result snippet copied below. Please answer YES or NO if it mentions the position: {role}?

    Snippet:
    {content[:12000]}
    """.strip())
    return vicuna_output


@click.command
@click.option('--debug', is_flag=True)
@click.option('--cache-dir', required=True, type=click.Path(dir_okay=True, file_okay=False, exists=True))
@click.argument('ROLE', type=str, required=True)
def main(role: str, debug: bool, cache_dir):
    basic_logging_config(debug)
    global log
    log = logging.getLogger(os.path.basename(__file__))
    cache = FilesystemCache(Path(cache_dir))

    openai = CachedOpenAI(
        cache=cache,
        openai_args={'api_key': 'EMPTY', 'base_url': 'http://localhost:17088/v1'},
        model_name='vicuna-7b-v1.5-16k'
    )

    search_engine_query = role
    payload = cache.get(
        key=search_engine_query,
        prefix_key='GOOGLE-',
        supplier=lambda: list(query_search_engine(search_engine_query))).payload
    for sr in payload:
        url = sr['url']
        try:
            raw_html = cache.get(key=url, prefix_key='HTTP_GET-', supplier=lambda: get_url(url)).payload
            article_text = _extract_article_text(raw_html)
            output = _llm_analyze_page_if_role_mentioned(openai, role, article_text)
            print(output)

            description = sr['description']
            output = _llm_analyze_snippet_if_role_mentioned(openai, role, description)
            print(description)
            print(output)

        except:
            log.exception(f'Error while processing {url}')


if __name__ == '__main__':
    main()
