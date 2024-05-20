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
