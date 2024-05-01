from typing import Callable, List

from s8er.cache import Cache


def create_cache(websearch_func: Callable[[str], List[str]],
                 cache: Cache[List[str]]) -> Callable[[], List[str]]:
    def _cache(query: str) -> List[str]:
        return cache.get_raw(query, lambda: websearch_func(query), prefix_key='WEBSEARCH-')

    return _cache
