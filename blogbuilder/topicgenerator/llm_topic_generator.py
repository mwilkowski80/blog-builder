import json
import re
from typing import List, Callable

from blogbuilder.llm import LLM

def create_generate_func(llm: LLM, search_queries_count: int) -> Callable[[], List[str]]:
    def _generate() -> List[str]:
        llm_output = llm(f"""
        I want to create a blog about financial crime and compliance. I consider this topic elementary and very important for wide public. People do not know what financial crime usually is and do not understand the procedures that banks have to undertake to deal with the problem. I want to fill this gap and allow regular people read and understand the topic of financial crime.

        I want to build this blog based on the input from the Internet and analyze the content available in the Internet already. I want to use DuckDuckGo search engine to look for interesting articles. Please generate a list of {search_queries_count} search queries that I should invoke in DuckDuckGo to find articles that I am interested in.

        Please return the results in the following JSON format:
        {{"queries": ["query 1", "query 2", "query 3"]}} and so on.

        with the above JSON in the first line.
""")

        return json.loads(re.search(r'\{\s*"queries".*}', llm_output, re.DOTALL).group(0))['queries']

    return _generate
