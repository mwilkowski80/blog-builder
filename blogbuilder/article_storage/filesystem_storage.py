import json
import logging
import os
from pathlib import Path
from typing import Iterable

from blogbuilder.article import Article
from blogbuilder.article_storage.articlestorage import ArticleStorage


class FilesystemStorage(ArticleStorage):
    def __init__(self, storage_dir: Path):
        self._storage_dir = storage_dir
        self._log = logging.getLogger(self.__class__.__name__)

    def contains(self, article_id: str) -> bool:
        article_path = self._id_to_path(article_id)
        return article_path.exists()

    def _id_to_path(self, article_id):
        article_path = self._storage_dir / (article_id + '.json')
        return article_path

    def get(self, article_id: str) -> Article:
        with open(self._id_to_path(article_id)) as f:
            d = json.load(f)
        return Article.from_dict(d)

    def put(self, article: Article) -> None:
        with open(self._id_to_path(article.id_), 'w') as f:
            json.dump(article.to_dict(), f)

    def get_all(self) -> Iterable[Article]:
        for fn in os.listdir(self._storage_dir):
            if fn.endswith('.json'):
                with open(self._storage_dir / fn) as f:
                    try:
                        article_dict = json.load(f)
                    except json.JSONDecodeError:
                        self._log.error(f'Error decoding {fn}')
                        raise
                yield Article.from_dict(article_dict)
