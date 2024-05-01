import logging
from datetime import date
from pathlib import Path
from typing import TextIO, Optional, List

import click
import yaml

from blogbuilder.article_storage.filesystem_storage import FilesystemStorage
from blogbuilder.generate_docusaurus_articles import GenerateDocusaurusArticlesUseCase
from blogbuilder.generate_markdown_articles import GenerateMarkdownArticle
from blogbuilder.generate_raw_articles_use_case import GenerateRawArticlesUseCase, PersistSummaryToFile
from blogbuilder.llm import OpenAILLM, LLM, LocalLLM
from s8er.cache import FilesystemCache
from s8er.llm import CachedOpenAI
from .wse import wse_create_cache, wse_google


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


@click.group()
@click.option('--debug', is_flag=True)
def cli(debug: bool):
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')


@cli.command('generate-raw-articles')
@click.option('--llm-endpoint', required=True)
@click.option('--cache-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--output-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--download-timeout', default=10)
def cli_local_llm(llm_endpoint: str, cache_dir: str, output_dir: str, download_timeout):
    cache_func = wse_create_cache(websearch_func=wse_google, cache=FilesystemCache(Path(cache_dir)))
    use_case = GenerateRawArticlesUseCase(llm=build_local_llm(llm_endpoint),
                                          persist_summary=PersistSummaryToFile(output_dir),
                                          websearch_func=cache_func, download_timeout=download_timeout)
    use_case.invoke()


@cli.command('generate-markdown-articles')
@click.option('--raw-articles-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--output-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--llm-endpoint', required=True)
@click.option('--max-number-of-articles', default=10)
@click.option('--max-retries-per-article', default=3)
def cli_generate_markdown_articles(
        raw_articles_dir: str, output_dir: str, llm_endpoint: str,
        max_number_of_articles: int, max_retries_per_article: int):
    GenerateMarkdownArticle(
        raw_articles_dir=raw_articles_dir, output_storage=FilesystemStorage(Path(output_dir)),
        llm=build_local_llm(llm_endpoint), max_number_of_articles=max_number_of_articles,
        max_retries_per_article=max_retries_per_article).invoke()


@cli.command('generate-docusaurus-articles')
@click.option('--markdown-articles-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--output-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
@click.option('--skip-existing', is_flag=True)
@click.option('--article-date', default=date.today().isoformat())
@click.option('--authors')
@click.option('--authors-yml-file', type=click.File(), required=False)
def cli_generate_docusaurus_articles(
        markdown_articles_dir: str, output_dir: str, skip_existing: bool,
        article_date: str, authors: Optional[str], authors_yml_file: Optional[TextIO]):
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
        article_date=date.fromisoformat(article_date), authors=_build_authors_list()).invoke()


if __name__ == '__main__':
    cli()
