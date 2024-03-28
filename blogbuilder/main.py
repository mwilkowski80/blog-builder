import re
from pathlib import Path
from time import time

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
@click.option('--output-dir', required=True, type=click.Path(dir_okay=True, exists=True, file_okay=False))
def cli_local_llm(llm_endpoint: str, output_dir: str):
    def _persist_summary_to_file(_summary: str, _context: str) -> None:
        def _sanitize_to_filename(s: str) -> str:
            sanitized = re.sub(r'\s+', '_', s)
            sanitized = re.sub(r'[<>:"/\\|?*]', '', sanitized)
            return sanitized

        output_filepath = Path(output_dir) / (_sanitize_to_filename(_context) + '-' + str(time()))
        with open(output_filepath, 'w') as f:
            f.write(_summary)

    use_case = BuildBlogUseCase(llm=build_local_llm(llm_endpoint), persist_summary_func=_persist_summary_to_file)
    use_case.invoke()


@cli.command('openai')
@click.option('--cache-dir', type=click.Path(
    dir_okay=True, file_okay=False, exists=True))
def cli_openai(cache_dir: str):
    use_case = BuildBlogUseCase(llm=build_openai_llm(cache_dir))
    use_case.invoke()


if __name__ == '__main__':
    cli()
