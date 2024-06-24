import csv
import traceback
from datetime import datetime
from pathlib import Path
from typing import List

import requests

from s8er.llm import CachedOpenAI


class LLM:
    def __call__(self, input_str) -> str:
        raise NotImplementedError()


class OpenAILLM(LLM):
    def __init__(self, openai: CachedOpenAI):
        self._openai = openai

    def __call__(self, input_str) -> str:
        return self._openai.query(input_str)


class LocalLLM(LLM):
    def __init__(self, generate_endpoint_url: str) -> None:
        self._generate_endpoint_url = generate_endpoint_url

    def __call__(self, input_str) -> str:
        r = requests.post(self._generate_endpoint_url, json={
            'input_text': input_str})
        r.raise_for_status()
        return r.json()['output']


class OllamaLLM(LLM):
    def __init__(self, endpoint: str, extra_args: dict) -> None:
        self._endpoint = endpoint
        self._extra_args = extra_args | {'stream': False}

    def __call__(self, input_str: str) -> str:
        r = requests.post(self._endpoint, json={'prompt': input_str, **self._extra_args})
        r.raise_for_status()
        return r.json()['response']


class NoopLLM(LLM):
    def __call__(self, input_str) -> str:
        return input_str


class LoggedLLM(LLM):
    def __init__(self, llm: LLM, log_filepath: Path, extra_args: List[str]) -> None:
        self._llm = llm
        self._log_filepath = log_filepath
        self._extra_args = extra_args

    def __call__(self, input_str) -> str:
        output = None
        try:
            output = self._llm(input_str)
            self._log(input_str=input_str, output_str=output)
            return output
        except:
            self._log(input_str=input_str,
                      output_str=output,
                      exception_str=traceback.format_exc())
            raise

    def _log(self, input_str: str, output_str: str, exception_str: str = None) -> None:
        with open(self._log_filepath, 'a') as f:
            writer = csv.writer(f)
            writer.writerow(self._extra_args + [datetime.utcnow().isoformat(), input_str, output_str, exception_str])
