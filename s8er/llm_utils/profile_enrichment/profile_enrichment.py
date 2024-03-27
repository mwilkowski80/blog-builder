from abc import ABC, abstractmethod
from typing import Callable, List, Dict, Any
from typing import Dict, Any
import os
from structlog.stdlib import get_logger as get_raw_logger

from s8er.llm_utils.halucination_detectors.selfcheck import BaseSelfValidator
from s8er.llm_utils.halucination_detectors.prompt_validation_templates import (
    IsSentenceSuportedByText,
    IsSentenceSuportedByText2,
    IsSentenceSuportedByContext,
    IsSentenceSuportedByContext2
)


logger = get_raw_logger(os.path.basename(__file__))


WEB_CONTENT_MAX_LEN = 12000


class AbstractProfileEnricher(ABC):
    prompt_validation_templates = [
        IsSentenceSuportedByText(),
        IsSentenceSuportedByText2(),
        IsSentenceSuportedByContext(),
        IsSentenceSuportedByContext2(),
    ]

    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__()

        self.llm_client = llm_client
        self.web_search_api_client = web_search_api_client
        self.validotor = BaseSelfValidator(
            llm_client=llm_client,
            web_search_api_client=web_search_api_client
        )
    
    @abstractmethod
    def enrich_entities(
        self,
        entities: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        return entities
