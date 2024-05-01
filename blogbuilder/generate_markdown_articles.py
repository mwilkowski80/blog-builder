import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import jinja2
from blogbuilder.article_storage.articlestorage import ArticleStorage

from blogbuilder.article import Article
from blogbuilder.llm import LLM


@dataclass
class Author:
    name: str
    title: Optional[str]
    url: Optional[str]
    image_url: Optional[str]


def _remove_first_line(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    text = text.strip()
    n_index = text.find('\n')
    if n_index >= 0:
        return text[n_index + 1:]
    else:
        return ''


def _remove_last_line(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    text = text.strip()
    n_index = text.rfind('\n')
    if n_index >= 0:
        return text[:n_index].strip()
    else:
        return ''


class GenerateMarkdownArticle:
    def __init__(self, raw_articles_dir: str, output_storage: ArticleStorage,
                 llm: LLM, max_number_of_articles: int, max_retries_per_article: int) -> None:
        self.output_storage = output_storage
        self._max_retries_per_article = max_retries_per_article
        self._max_number_of_articles = max_number_of_articles
        self._llm = llm
        self._raw_articles_dir = raw_articles_dir
        self._log = logging.getLogger(self.__class__.__name__)

    def invoke(self) -> None:
        self._log.info(f'Generating blog articles from {self._raw_articles_dir}')
        counter = 0
        for filename in os.listdir(self._raw_articles_dir):
            if self.output_storage.contains(filename):
                self._log.debug(f'Skipping {filename} as it already exists')
                continue

            self.process_input_file(filename)
            counter += 1
            if counter >= self._max_number_of_articles:
                self._log.info(f'Generated {counter} articles, stopping')
                break

    def process_input_file(self, filename):
        with open(os.path.join(self._raw_articles_dir, filename), 'r') as f:
            raw_article = f.read()

        self._log.info(f'Generating article markdown for {filename}')
        article_markdown = self._generate_blog_article_markdown(raw_article)
        self._log.info(f'Generating article title {filename}')
        article_title = self._generate_title(article_markdown)
        self._log.info(f'Article title is: {article_title}')
        article_category = GenerateMarkdownArticle._category_from_filename(filename)
        self.output_storage.put(Article(
            id_=filename,
            title=article_title,
            content=article_markdown,
            tags=[article_category] if article_category else [],
            generated_at=datetime.utcnow(),
        ))

    def _generate_blog_article_markdown(self, raw_article_text: str) -> str:
        prompt = f"""Please convert the given article text into an article following markdown format. Please generate proper headings, subheadings, and bullet points.
        
        Here is the raw article text:
        {raw_article_text}
        """
        llm_response = self._llm(prompt)
        if llm_response.startswith('```'):
            llm_response = _remove_first_line(llm_response)
        if llm_response.endswith('```'):
            llm_response = _remove_last_line(llm_response)
        return llm_response

    def _generate_title(self, article_text: str) -> str:
        for i in range(0, self._max_retries_per_article):
            llm_response = None
            try:
                llm_response = self._generate_title_llm(article_text)
                return re.search(r'\{\s*"title"\s*:\s*"(.*?)"\s*}', llm_response).group(1)
            except Exception as e:
                self._log.exception(f'Failed to parse title from LLM response: {llm_response}')
        raise Exception(f'Failed to generate title for article after {self._max_retries_per_article} retries')

    def _generate_title_llm(self, article_text: str) -> str:
        prompt = f"""Please generate a title for an article. Generated title must be short (not more than 5 words) and attractive. Please generate it in the following JSON format:
        {{"title":"<title>"}}
        
        Here is the raw article text:
        {article_text}
        """
        return self._llm(prompt)

    def _generate_article_header(self, tags: List[str], article_title: str, author_name: str) -> str:
        return jinja2.Template("""---
title: {{article_title}}
description: {{article_title}}
authors: {{author_name}}
{% if tags %}    tags: [{% for tag in tags %}{{ tag }}{% if not loop.last %},{% endif %}{% endfor %}]
{% endif %}---
""").render(article_title=article_title, author_name=author_name, tags=tags).strip()

    @staticmethod
    def _category_from_filename(filename: str) -> Optional[str]:
        dash_index = filename.rfind('-')
        if dash_index >= 0:
            return filename[:dash_index].replace(' ', '-')
