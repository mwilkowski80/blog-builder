import logging
import re
from datetime import date
from pathlib import Path
from random import randint
from subprocess import run, check_call, PIPE
from typing import List, Tuple

import yaml

from blogbuilder.article import Article
from blogbuilder.article_storage.articlestorage import ArticleStorage


def _sanitize_markdown(value: str) -> str:
    return re.sub(r'<(https?://[^>]+)>', r'[\1](\1)', value)


def _sanitize_yaml_value(value: str) -> str:
    return value.replace(':', '-').replace('\n', '-') if value else value


def _sanitize_to_slug(text: str) -> str:
    # Convert text to lowercase
    text = text.lower()
    # Replace any non-word character (not a letter, digit or underscore) with a dash
    text = re.sub(r'\W+', '-', text)
    # Replace multiple dashes with a single dash
    text = re.sub(r'-+', '-', text)
    # Strip dashes from the start and end of the slug
    text = text.strip('-')
    return text


def _extract_timestamp_from_article_id(article_id: str) -> str:
    last_str = article_id.split('-')[-1]
    return last_str.replace('.', '')


class GenerateDocusaurusArticlesUseCase:
    def __init__(
            self, article_storage: ArticleStorage, output_dir: Path, skip_existing: bool,
            article_date: date, authors: List[str], max_attempts: int) -> None:
        self._max_attempts = max_attempts
        self._article_storage = article_storage
        self._output_dir = output_dir
        self._skip_existing = skip_existing
        self._log = logging.getLogger(self.__class__.__name__)
        self._article_date = article_date
        if not authors:
            raise ValueError('At least one author is required')
        self._article_author = authors

    def invoke(self):
        for article in self._article_storage.get_all():
            output_filepath = self._output_dir / f'{article.id_}.md'
            if self._skip_existing and output_filepath.exists():
                continue
            self._write_article(article)

        self._rebuild_static_files()

    def _write_article(self, article: Article) -> None:
        slug = _sanitize_to_slug(article.title) + '-' + _extract_timestamp_from_article_id(article.id_)
        output_filepath = self._output_dir / f'{article.id_}.md'
        with open(output_filepath, 'w') as f:
            f.write(f'---\n')
            yaml.dump({
                'title': article.title,
                'description': article.title,
                'authors': self._select_random_author(),
                'date': self._article_date.isoformat(),
                'slug': slug,
            }, f)
            f.write(f'---\n')
            f.write(_sanitize_markdown(article.content))
            f.write('\n')

    def _try_unlink(self, filepath: Path) -> None:
        if filepath.exists():
            filepath.unlink()
            self._log.error(f'Deleted {filepath} because of error')

    def _rebuild_static_files(self) -> None:
        self._log.info('_rebuild_static_files() invoked')

        for i in range(0, self._max_attempts):
            return_code, stderr_output = self._check_stderr_output()
            if return_code == 0:
                self._log.info(f'Rebuilt static files successfully. Run index: {i}')
                return

            self._log.info('Errors found, trying to fix them')
            for line in stderr_output.splitlines():
                m = re.search(r'Error: MDX compilation failed for file "(.+?)"', line)
                if m:
                    error_filepath = Path(m.group(1))
                    self._try_unlink(error_filepath)
                m = re.search(r'Error: Can\'t render static file for pathname "/(.+?)"', line)
                if m:
                    slug = m.group(1)
                    self._try_delete_slug(slug)
                m = re.search(r'Broken link on source page path = /(.+?):', line)
                if m:
                    slug = m.group(1)
                    self._try_delete_slug(slug)
                m = re.search(r'Image .+? used in blog/(.+)? not found', line)
                if m:
                    filename = m.group(1)
                    self._try_delete_filename(filename)

        raise Exception('Failed to rebuild static files. Last index: {i}')

    def _try_delete_slug(self, slug: str):
        for filepath in self._find_files_with_slug(slug):
            self._try_unlink(filepath=filepath)
            error_filepath = self._output_dir / (slug + '.md')
            self._try_unlink(filepath=error_filepath)

    def _check_stderr_output(self) -> Tuple[int, str]:
        run_output = run(['npm', 'run', 'build'], cwd=self._output_dir, text=True, stderr=PIPE)
        return run_output.returncode, run_output.stderr

    def _select_random_author(self) -> str:
        return self._article_author[randint(0, len(self._article_author) - 1)]

    def _find_files_with_slug(self, slug: str) -> List[Path]:
        output = []
        run_output = run(['grep', '-irP', f'^slug: {slug}$', self._output_dir], text=True, stdout=PIPE).stdout
        for line in run_output.splitlines():
            output.append(Path(line.split(':')[0]))
        return output

    def _try_delete_filename(self, filename: str) -> None:
        output_filepath = self._output_dir / filename
        self._try_unlink(filepath=output_filepath)
