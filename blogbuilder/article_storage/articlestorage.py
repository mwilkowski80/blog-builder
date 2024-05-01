from typing import Iterable

from blogbuilder.article import Article


class ArticleStorage:
    def contains(self, article_id: str) -> bool:
        raise NotImplementedError()

    def get(self, article_id: str) -> Article:
        raise NotImplementedError()

    def put(self, article: Article) -> None:
        raise NotImplementedError()

    def get_all(self) -> Iterable[Article]:
        raise NotImplementedError()
