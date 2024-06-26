import json
import logging
from datetime import date
from pathlib import Path
from typing import TextIO, Optional, List

import click
import yaml

from blogbuilder.article_storage.filesystem_storage import FilesystemStorage
from blogbuilder.generate_docusaurus_articles import GenerateDocusaurusArticlesUseCase, SanitizeOperations
from blogbuilder.generate_hugo_articles import GenerateHugoArticlesUseCase
from blogbuilder.generate_markdown_articles import GenerateMarkdownArticle
from blogbuilder.generate_raw_articles_use_case import GenerateRawArticlesUseCase, PersistSummaryToFile, \
    GenerateRawArticles2UseCase
from blogbuilder.llm import OpenAILLM, LLM, LocalLLM, OllamaLLM, LoggedLLM
from blogbuilder.obtaincontent import obtain_content_from_url_func
from s8er.cache import FilesystemCache
from s8er.llm import CachedOpenAI
from .wse import wse_create_cache, wse_google, wse_ddgs_create_func
from .topicgenerator import llm_topic_generator_create_func, per_region_topic_generator_create_func


def build_openai_llm(cache_dir: str) -> LLM:
    if not cache_dir:
        raise ValueError('--cache-dir parameter is required')

    cache = FilesystemCache(Path(cache_dir))
    openai = CachedOpenAI(
        cache=cache,
        openai_args={'api_key': 'EMPTY', 'base_url': 'http://localhost:17088/v1'},
        model_name='vicuna-7b-v1.5-16k'
    )
    return OpenAILLM(openai=openai)


def build_local_llm(llm_endpoint: str) -> LLM:
    return LocalLLM(llm_endpoint)


def build_ollama_endpoint(endpoint: str, extra_args: dict) -> LLM:
    return OllamaLLM(endpoint=endpoint, extra_args=extra_args)


@click.group()
@click.option('--debug', is_flag=True)
def cli(debug: bool):
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')


WEB_SEARCH_ENGINE_MAP = {
    'google': wse_google,
    'ddg': wse_ddgs_create_func(20),
}


@cli.command('generate-raw-articles')
@click.option('--llm-endpoint')
@click.option('--ollama-endpoint')
@click.option('--ollama-extra-args')
@click.option('--llm-log-file', type=click.Path(dir_okay=False, file_okay=True))
@click.option('--cache-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--output-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--download-timeout', default=10)
@click.option('--wse', default='google', type=click.Choice(list(WEB_SEARCH_ENGINE_MAP.keys())))
@click.option('--topic-generator', default='per_country_llm', type=click.Choice(['llm', 'per_country_llm']))
@click.option('--topic-generator-max-search-queries', default=30)
@click.option('--max-llm-payload', default=12000)
@click.option('--sample-countries-count', default=5)
@click.option('--version', type=click.Choice(['v1', 'v2']), default='v2')
def cli_generate_raw_articles(llm_endpoint: str, ollama_endpoint: str, ollama_extra_args: str,
                              cache_dir: str, output_dir: str, download_timeout: int,
                              wse: str, topic_generator: str, max_llm_payload: int,
                              topic_generator_max_search_queries: int, sample_countries_count: int,
                              version: str, llm_log_file: Optional[str]):
    llm = build_llm_from_args(llm_endpoint=llm_endpoint,
                              ollama_endpoint=ollama_endpoint,
                              ollama_extra_args=ollama_extra_args,
                              llm_log_file=llm_log_file)

    if topic_generator == 'llm':
        topic_generator_func = llm_topic_generator_create_func(
            llm, topic_generator_max_search_queries)
    elif topic_generator == 'per_country_llm':
        inner_generator_func = llm_topic_generator_create_func(
            llm, topic_generator_max_search_queries)
        topic_generator_func = per_region_topic_generator_create_func(
            inner_generator_func, sample_countries_count)
    else:
        raise ValueError(f'Unknown topic generator: {topic_generator}')

    cache = FilesystemCache(Path(cache_dir))
    cache_func = wse_create_cache(websearch_func=WEB_SEARCH_ENGINE_MAP[wse], cache=cache)

    kwargs = dict()
    if version == 'v2':
        use_case_class = GenerateRawArticles2UseCase
        kwargs['obtain_content_func'] = obtain_content_from_url_func(timeout=download_timeout)
    else:
        use_case_class = GenerateRawArticlesUseCase

    use_case = use_case_class(
        llm=llm, persist_summary=PersistSummaryToFile(output_dir),
        websearch_func=cache_func, download_timeout=download_timeout, topic_generator_func=topic_generator_func,
        check_cache=cache, max_llm_payload=max_llm_payload, **kwargs)
    use_case.invoke()


def build_llm_from_args(llm_endpoint: Optional[str], ollama_endpoint: Optional[str],
                        ollama_extra_args: Optional[str], llm_log_file: Optional[str]) -> LLM:
    if llm_endpoint and ollama_endpoint:
        raise ValueError('Only one of --llm-endpoint or --ollama-endpoint can be specified')
    if not llm_endpoint and not ollama_endpoint:
        raise ValueError('One of --llm-endpoint or --ollama-endpoint must be specified')
    if llm_endpoint:
        llm = build_local_llm(llm_endpoint)
    else:
        ollama_extra_args = ollama_extra_args or '{}'
        llm = build_ollama_endpoint(ollama_endpoint, json.loads(ollama_extra_args))

    if llm_log_file:
        llm = LoggedLLM(llm, Path(llm_log_file), [])
    return llm


@cli.command('generate-markdown-articles')
@click.option('--raw-articles-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--output-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--llm-endpoint')
@click.option('--ollama-endpoint')
@click.option('--ollama-extra-args')
@click.option('--llm-log-file', type=click.Path(dir_okay=False, file_okay=True))
@click.option('--max-number-of-articles', default=10)
@click.option('--max-retries-per-article', default=3)
@click.option('--max-llm-payload', default=12000)
def cli_generate_markdown_articles(
        raw_articles_dir: str, output_dir: str, llm_endpoint: str,
        ollama_endpoint: str, ollama_extra_args: str,
        max_number_of_articles: int, max_retries_per_article: int,
        max_llm_payload: int, llm_log_file: Optional[str]):
    llm = build_llm_from_args(llm_endpoint=llm_endpoint,
                              ollama_endpoint=ollama_endpoint,
                              ollama_extra_args=ollama_extra_args,
                              llm_log_file=llm_log_file)
    GenerateMarkdownArticle(
        raw_articles_dir=raw_articles_dir, output_storage=FilesystemStorage(Path(output_dir)),
        llm=llm, max_number_of_articles=max_number_of_articles,
        max_retries_per_article=max_retries_per_article, max_llm_payload=max_llm_payload).invoke()


@cli.command('generate-docusaurus-articles')
@click.option('--markdown-articles-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--output-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--operation-log-path', required=True, type=click.Path(dir_okay=False, file_okay=True, exists=True))
@click.option('--skip-existing', is_flag=True)
@click.option('--article-date', default=date.today().isoformat())
@click.option('--authors')
@click.option('--authors-yml-file', type=click.File(), required=False)
@click.option('--max-attempts', default=10)
def cli_generate_docusaurus_articles(
        markdown_articles_dir: str, output_dir: str, skip_existing: bool,
        article_date: str, authors: Optional[str], authors_yml_file: Optional[TextIO],
        max_attempts: int, operation_log_path: str):
    def _build_authors_list() -> List[str]:
        if authors:
            return authors.split(',')
        elif authors_yml_file:
            return list(yaml.safe_load(authors_yml_file).keys())
        else:
            raise ValueError('At least one author is required')

    GenerateDocusaurusArticlesUseCase(
        article_storage=FilesystemStorage(Path(markdown_articles_dir)),
        output_dir=Path(output_dir), skip_existing=skip_existing,
        article_date=date.fromisoformat(article_date), authors=_build_authors_list(),
        max_attempts=max_attempts,
        sanitize_operations=SanitizeOperations(output_dir=Path(output_dir), operation_log_path=Path(operation_log_path))
    ).invoke()


@cli.command('generate-hugo-articles')
@click.option('--markdown-articles-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--output-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--skip-existing', is_flag=True)
@click.option('--article-date', default=date.today().isoformat())
@click.option('--max-attempts', default=10)
@click.option('--authors')
@click.option('--authors-yml-file', type=click.File(), required=False)
def cli_generate_hugo_articles(
        markdown_articles_dir: str, output_dir: str, skip_existing: bool,
        article_date: str, authors: Optional[str], authors_yml_file: Optional[TextIO],
        max_attempts: int):
    def _build_authors_list() -> List[str]:
        if authors:
            return authors.split(',')
        elif authors_yml_file:
            return list(yaml.safe_load(authors_yml_file).keys())
        else:
            raise ValueError('At least one author is required')

    GenerateHugoArticlesUseCase(
        article_storage=FilesystemStorage(Path(markdown_articles_dir)),
        output_dir=Path(output_dir), skip_existing=skip_existing,
        article_date=date.fromisoformat(article_date), authors=_build_authors_list(),
        max_attempts=max_attempts,
    ).invoke()


if __name__ == '__main__':
    cli()
