from typing import List

from googlesearch import search


def invoke(query: str) -> List[str]:
    return list(search(query))
