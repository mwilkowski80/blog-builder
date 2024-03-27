from pathlib import Path

import click

from blogbuilder.llm import OpenAILLM, LLM, LocalLLM
from blogbuilder.use_case import BuildBlogUseCase
from s8er.cache import FilesystemCache
from s8er.llm import CachedOpenAI


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
def cli():
    pass


@cli.command('local-llm')
@click.option('--llm-endpoint')
def cli_local_llm(llm_endpoint: str):
    use_case = BuildBlogUseCase(llm=build_local_llm(llm_endpoint))
    use_case.invoke()


@cli.command('openai')
@click.option('--cache-dir', type=click.Path(
    dir_okay=True, file_okay=False, exists=True))
def cli_openai(cache_dir: str):
    use_case = BuildBlogUseCase(llm=build_openai_llm(cache_dir))
    use_case.invoke()


if __name__ == '__main__':
    cli()
