from typing import List, Callable

from duckduckgo_search import DDGS


def create_search_func(max_results: int) -> Callable[[str], List[str]]:
    def _search(query: str) -> List[str]:
        results = DDGS().text(query, max_results=max_results)
        return [result['href'] for result in results]

    return _search
