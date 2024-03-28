import json
from typing import List, Callable

import requests
from duckduckgo_search import DDGS

from blogbuilder.llm import LLM


class BuildBlogUseCase:
    def __init__(self, llm: LLM, persist_summary_func: Callable[[str, str], None]) -> None:
        self._llm = llm
        self._persist_summary_func = persist_summary_func

    def _get_queries_from_llm(self) -> List[str]:
        queries_str = self._llm("""
        I want to create a blog about financial crime and compliance. I consider this topic elementary and very important for wide public. People do not know what financial crime usually is and do not understand the procedures that banks have to undertake to deal with the problem. I want to fill this gap and allow regular people read and understand the topic of financial crime.

        I want to build this blog based on the input from the Internet and analyze the content available in the Internet already. I want to use DuckDuckGo search engine to look for interesting articles. Please generate an exhaustive list of search queries that I should invoke in DuckDuckGo to find articles that I am interested in.

        Please return the results in the following JSON format:
        {"queries": ["query 1", "query 2", "query 3"]} and so on.

        I want to consume the output automatically, therefore please do not return anything else.
                """)
        return json.loads(queries_str)['queries']

    def _get_queries_from_text(self) -> List[str]:
        return [
            "what is financial crime", "types of financial crime", "examples of financial crime",
            "financial crime prevention", "financial crime compliance", "anti-money laundering", "AML procedures",
            "know your customer KYC", "suspicious activity reports", "financial crime investigations",
            "financial crime regulations", "financial crime laws", "financial crime enforcement",
            "financial crime penalties", "financial crime statistics", "financial crime trends",
            "financial crime case studies", "financial crime news", "financial crime blogs", "financial crime podcasts",
            "financial crime books", "financial crime documentaries", "financial crime awareness",
            "financial crime education", "financial crime training", "financial crime jobs", "financial crime careers",
            "financial crime technology", "financial crime software", "financial crime tools",
            "financial crime risk assessment", "financial crime risk management", "financial crime compliance programs",
            "financial crime compliance best practices", "financial crime compliance challenges",
            "financial crime compliance costs", "financial crime compliance trends", "financial crime compliance news",
            "financial crime compliance blogs", "financial crime compliance podcasts",
            "financial crime compliance books", "financial crime compliance certifications",
            "financial crime compliance jobs", "financial crime compliance careers",
            "financial institutions and crime prevention", "banks and financial crime",
            "role of banks in preventing financial crime", "how banks detect financial crime", "bank secrecy act",
            "USA PATRIOT Act", "FATF recommendations", "Basel AML Index", "FinCEN", "OFAC sanctions",
            "financial crime in the cryptocurrency industry", "money laundering through real estate",
            "trade-based money laundering", "financial crime and terrorist financing",
            "financial crime and organized crime", "financial crime and corruption", "financial crime and tax evasion",
            "financial crime and fraud", "financial crime and cybercrime", "financial crime and virtual currencies",
            "financial crime and offshore banking", "financial crime and shell companies",
            "financial crime and politically exposed persons (PEPs)", "financial crime and beneficial ownership",
            "financial crime and customer due diligence (CDD)", "financial crime and enhanced due diligence (EDD)",
            "financial crime and risk-based approach", "financial crime and transaction monitoring",
            "financial crime and sanctions screening", "financial crime and adverse media screening",
            "financial crime and red flags", "financial crime and typologies", "financial crime and emerging threats",
            "financial crime and regulatory compliance", "financial crime and corporate governance",
            "financial crime and ethics", "financial crime and social responsibility",
            "impact of financial crime on society", "cost of financial crime", "fighting financial crime",
            "preventing financial crime", "detecting financial crime", "reporting financial crime",
            "investigating financial crime", "prosecuting financial crime"]

    def invoke(self) -> None:
        queries = self._get_queries_from_text()
        with DDGS() as ddgs:
            for query in queries:
                results = ddgs.text(query)
                for r in results:
                    href = r['href']
                    if __name__ == '__main__':
                        r = requests.get(href)
                        r.raise_for_status()
                        page_html = r.text
                        if self._check_if_page_is_related_to_phrase(page_html, query):
                            summary = self._summarize_the_page_for_me(page_html, query)
                            self._capture_summary(query, summary)

    def _check_if_page_is_related_to_phrase(self, page_html: str, query: str) -> bool:
        llm_query = f"""
I want to create a blog about financial crime compliance. Here is a topic that I want to write about: {query}.

Please check the content of the page below and evaluate how much this topic is related to the page content. Please answer using only a single integer number in scale 0-100 where 0 means completely unrelated and 100 means fully related. Examples:
0
30
100
73

Here is the content of the page:
{page_html}"""
        output = self._llm(llm_query)
        return int(output) > 50

    def _summarize_the_page_for_me(self, page_html: str, topic: str) -> str:
        llm_query = f"""
I am preparing a blog about the financial crime compliance. I want to specifically write about this topic: {topic}.

Please summarize the following web page content in the context of that topic:
f{page_html}"""
        return self._llm(llm_query)

    def _capture_summary(self, query: str, summary: str) -> None:
        self._persist_summary_func(summary, query)