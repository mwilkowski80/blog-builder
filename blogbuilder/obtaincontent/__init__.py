import io
import os
import tempfile
from pathlib import Path
from subprocess import check_output

import pypdf
import requests

from typing import Callable


def obtain_content_from_url_func(timeout: int) -> Callable[[str], str]:
    def _extract_article_using_readibility(article_text: str) -> str:
        with tempfile.NamedTemporaryFile(mode='w') as f:
            f.write(article_text)
            f.flush()
            url = f.name
            return check_output(['node', 'extract-article.js', '--url', url], text=True,
                                cwd=Path(os.getcwd()) / 'extract-article')

    def _obtain_content_from_url(url: str) -> str:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()

        if r.headers.get('content-type') in ['application/pdf', 'application/octet-stream']:
            reader = pypdf.PdfReader(io.BytesIO(r.content))
            return '\n'.join([page.extract_text() for page in reader.pages])
        else:
            return _extract_article_using_readibility(r.text)

    return _obtain_content_from_url
