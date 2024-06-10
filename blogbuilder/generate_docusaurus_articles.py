import json
import logging
import os
import re
from datetime import date
from pathlib import Path
from random import randint
from subprocess import run, PIPE
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


class SanitizeOperations:
    def __init__(self, output_dir: Path, operation_log_path: Path) -> None:
        self._output_dir = output_dir
        self._log = logging.getLogger(self.__class__.__name__)
        self._operation_log_path = operation_log_path

    def run_history(self) -> None:
        with open(self._operation_log_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    operation_dict = json.loads(line)
                    self._process_operation(operation_dict)

    def unlink_filepath(self, filepath: Path) -> None:
        self._register_and_process_operation({'name': 'unlink_filepath', 'filepath': str(filepath)})

    def unlink_filename(self, filename: str) -> None:
        self._register_and_process_operation({'name': 'unlink_filename', 'filename': filename})

    def unlink_slug(self, slug: str) -> None:
        self._register_and_process_operation({'name': 'unlink_slug', 'slug': slug})

    def _unlink_filepath(self, filepath: str) -> None:
        filepath = Path(filepath)
        if filepath.exists():
            filepath.unlink()

    def _unlink_filename(self, filename: str) -> None:
        output_filepath = self._output_dir / filename
        self.unlink_filepath(filepath=output_filepath)

    def _unlink_slug(self, slug: str):
        for filepath in self._find_files_with_slug(slug):
            self.unlink_filepath(filepath=filepath)
            error_filepath = self._output_dir / (slug + '.md')
            self.unlink_filepath(filepath=error_filepath)

    def _find_files_with_slug(self, slug: str) -> List[Path]:
        output = []
        run_output = run(['grep', '-irP', f'^slug: {slug}$', self._output_dir], text=True, stdout=PIPE).stdout
        for line in run_output.splitlines():
            output.append(Path(line.split(':')[0]))
        return output

    def _process_operation(self, operation_dict: dict) -> None:
        self._log.info('Invoking operation: %s', operation_dict)
        operation_name = operation_dict.get('name')
        method_name = '_' + operation_name
        method = getattr(self, method_name, None)
        operation_dict.pop('name', None)
        method(**operation_dict)

    def _register_operation(self, operation_json: dict) -> None:
        with open(self._operation_log_path, 'a') as f:
            f.write(json.dumps(operation_json) + '\n')

    def _register_and_process_operation(self, operation_json: dict) -> None:
        self._register_operation(operation_json)
        self._process_operation(operation_json)


class GenerateDocusaurusArticlesUseCase:
    def __init__(
            self, article_storage: ArticleStorage, output_dir: Path, skip_existing: bool,
            article_date: date, authors: List[str], max_attempts: int,
            sanitize_operations: SanitizeOperations) -> None:
        self._max_attempts = max_attempts
        self._article_storage = article_storage
        self._output_dir = output_dir
        self._skip_existing = skip_existing
        self._log = logging.getLogger(self.__class__.__name__)
        self._article_date = article_date
        self._sanitize_operations = sanitize_operations
        self._build_dir = output_dir / '..' / 'build'
        if not authors:
            raise ValueError('At least one author is required')
        self._article_author = authors

    def invoke(self):
        for article in self._article_storage.get_all():
            output_filepath = self._output_dir / f'{article.id_}.md'
            if self._skip_existing and output_filepath.exists():
                continue
            self._write_article(article)

        self._sanitize_operations.run_history()
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

    def _build_directory_contains_files(self) -> bool:
        return len(os.listdir(self._build_dir)) > 10

    def _rebuild_static_files(self) -> None:
        self._log.info('_rebuild_static_files() invoked')

        for i in range(0, self._max_attempts):
            self._log.info(f'Rebuilding static files. Run index: {i}')
            return_code, stderr_output = self._check_stderr_output()
            if return_code == 0:
                self._log.info(f'Rebuilt static files successfully. Run index: {i}')
                return

            if self._build_directory_contains_files():
                self._log.info('Build directory contains files, we can finish rebuild process')
                return

            self._log.info('Errors found, trying to fix them')
            for line in stderr_output.splitlines():
                m = re.search(r'Error: MDX compilation failed for file "(.+?)"', line)
                if m:
                    error_filepath = Path(m.group(1))
                    self._sanitize_operations.unlink_filepath(error_filepath)
                m = re.search(r'Error: Can\'t render static file for pathname "/(.+?)"', line)
                if m:
                    slug = m.group(1)
                    self._sanitize_operations.unlink_slug(slug)
                m = re.search(r'Broken link on source page path = /(.+?):', line)
                if m:
                    slug = m.group(1)
                    self._sanitize_operations.unlink_slug(slug)
                m = re.search(r'Image .+? used in blog/(.+)? not found', line)
                if m:
                    filename = m.group(1)
                    self._sanitize_operations.unlink_filename(filename)

        raise Exception(f'Failed to rebuild static files. Max attempts: {self._max_attempts}')

    def _check_stderr_output(self) -> Tuple[int, str]:
        run_output = run(['npm', 'run', 'build'], cwd=self._output_dir, text=True, stderr=PIPE)
        return run_output.returncode, run_output.stderr

    def _select_random_author(self) -> str:
        return self._article_author[randint(0, len(self._article_author) - 1)]
