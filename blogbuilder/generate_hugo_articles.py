import logging
import os
import re
from datetime import date
from pathlib import Path
from random import randint
from subprocess import run, PIPE, STDOUT
from typing import List, Tuple

import yaml

from blogbuilder.article import Article
from blogbuilder.article_storage.articlestorage import ArticleStorage
from blogbuilder.util import sanitize_to_slug, extract_timestamp_from_article_id


class GenerateHugoArticlesUseCase:
    def __init__(
            self, article_storage: ArticleStorage, output_dir: Path, skip_existing: bool,
            article_date: date, authors: List[str], max_attempts: int) -> None:
        self._article_storage = article_storage
        self._output_dir = output_dir
        self._skip_existing = skip_existing
        self._log = logging.getLogger(self.__class__.__name__)
        self._article_date = article_date
        self._build_dir = output_dir / '..' / 'build'
        self._max_attempts = max_attempts
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
        slug = sanitize_to_slug(article.title) + '-' + extract_timestamp_from_article_id(article.id_)
        output_filepath = self._output_dir / 'content' / 'posts' / f'{article.id_}.md'
        with open(output_filepath, 'w') as f:
            f.write(f'---\n')
            yaml.dump({
                'title': article.title,
                'author': self._select_random_author(),
                'date': self._article_date.isoformat(),
                'slug': slug,
            }, f)
            f.write(f'---\n')
            f.write(article.content)
            f.write('\n')

    def _build_directory_contains_files(self) -> bool:
        return len(os.listdir(self._build_dir)) > 10

    def _rebuild_static_files(self) -> None:
        attempt_index = 1
        while True:
            if attempt_index > self._max_attempts:
                raise RuntimeError('Max attempts reached')
            self._log.info(f'Rebuilding static files. Run index: {attempt_index}')
            error_code, stderr_output = self._check_stderr_output()
            if error_code == 0:
                self._log.info(f'Rebuilt static files successfully. Run index: {attempt_index}')
                return

            for line in stderr_output.splitlines():
                m = re.search(f'error building site.*"({self._output_dir}.*md):[0-9]+:[0-9]+"', line)
                if m:
                    os.remove(m.group(1))

            attempt_index += 1

    def _check_stderr_output(self) -> Tuple[int, str]:
        run_output = run(['hugo'], cwd=self._output_dir, text=True, stderr=STDOUT, stdout=PIPE)
        return run_output.returncode, run_output.stdout

    def _select_random_author(self) -> str:
        return self._article_author[randint(0, len(self._article_author) - 1)]
