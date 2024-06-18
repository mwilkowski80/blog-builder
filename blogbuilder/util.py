import re


def sanitize_to_slug(text: str) -> str:
    # Convert text to lowercase
    text = text.lower()
    # Replace any non-word character (not a letter, digit or underscore) with a dash
    text = re.sub(r'\W+', '-', text)
    # Replace multiple dashes with a single dash
    text = re.sub(r'-+', '-', text)
    # Strip dashes from the start and end of the slug
    text = text.strip('-')
    return text


def extract_timestamp_from_article_id(article_id: str) -> str:
    last_str = article_id.split('-')[-1]
    return last_str.replace('.', '')
