from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Article:
    id_: str
    title: str
    content: str
    tags: list[str]
    generated_at: Optional[datetime]

    @classmethod
    def from_dict(cls, d: dict) -> 'Article':
        return Article(
            id_=d['id'],
            title=d['title'],
            content=d['content'],
            tags=d['tags'],
            generated_at=datetime.fromisoformat(d['generated_at']),
        )

    def to_dict(self) -> dict:
        return {
            'id': self.id_,
            'title': self.title,
            'content': self.content,
            'tags': self.tags,
            'generated_at': self.generated_at.isoformat(),
        }