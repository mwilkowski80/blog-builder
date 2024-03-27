from abc import ABC, abstractmethod
from typing import Callable, List, Dict
import os
from structlog.stdlib import get_logger as get_raw_logger
from datetime import date
import bs4
from itertools import chain

from s8er.llm_utils.commons import ask_chat, validate_name, get_url, answer_to_bool, CHAT_FALSE_VALUES, CHAT_TRUE_VALUES


logger = get_raw_logger(os.path.basename(__file__))


WEB_CONTENT_MAX_DAYS_OLD = 365 * 5


class AbstarctPositionValidator(ABC):
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__()

        self.llm_client = llm_client
        self.web_search_api_client = web_search_api_client

    @abstractmethod
    def validate_entities(self, extracted_entities: List[dict]):
        pass

    @staticmethod
    def validate_web_content_date(web_content_date: str) -> bool:
        if web_content_date is None:
            return True
        return (date.today() - date.fromisoformat(web_content_date)).days < WEB_CONTENT_MAX_DAYS_OLD


class StandardPositionValidator(AbstarctPositionValidator):
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__(llm_client, web_search_api_client)

    def validate_entities(self, extracted_entities: List[dict]):
        return extracted_entities


class StateGovernmentPositionValidator(AbstarctPositionValidator):
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__(llm_client, web_search_api_client)

    def validate_entities(self, extracted_entities: List[dict]) -> chain:
        validated_entities = []
        for state in extracted_entities:

            validated_entities.extend(
                self.validate_entities_found(
                    state["entities_found"],
                    state["country"],
                    state["state_type"],
                    state["group_type"],
                    state["state_name"],
                )
            )
        return chain.from_iterable(validated_entities)

    def validate_entities_found(
        self,
        entities_found: List[dict],
        country: str,
        state_type: str,
        group_type: str,
        state_name: str,
    ) -> List[dict]:
        validated_entities = []

        for web_source_entities in entities_found:
            
            if "answer_available" in web_source_entities:
                answer_available = web_source_entities["answer_available"]
            elif "answer-available" in web_source_entities:
                answer_available = web_source_entities["answer-available"]
            elif "answer available" in web_source_entities:
                answer_available = web_source_entities["answer available"]
            else:
                answer_available = False
            
            if (answer_available in CHAT_TRUE_VALUES) \
                and len(web_source_entities.get("answer", [])) > 0:

                validated_entities.append(
                    self.validate_single_web_source(
                        web_source_entities,
                        country,
                        state_type,
                        group_type,
                        state_name,
                    )
                )
        return validated_entities

    def validate_single_web_source(
        self,
        web_source_entities: List[Dict[str, str]],
        country: str,
        state_type: str,
        group_type: str,
        state_name: str,
    ):
        
        if not self.validate_web_content_date(web_source_entities["web_content_date"]):
            return []
        
        web_content = bs4.BeautifulSoup(
            get_url(web_source_entities["url"]),
            features="lxml"
        ).text
        
        entities_from_source = [
            self.validate_single_entity(
                chat_answer,
                country,
                state_type,
                group_type,
                state_name,
                web_content,
            ) for chat_answer in web_source_entities["answer"]
        ]
        
        entities_meta = {
            "source_url": web_source_entities["url"],
            "source_date": web_source_entities["web_content_date"],
        }
        
        return [entity | entities_meta for entity in entities_from_source if entity is not None]

    def validate_single_entity(
        self,
        chat_answer,
        country: str,
        state_type,
        group_type: str,
        state_name: str,
        web_content: str,
    ):  # TODO: this method is too long!
        
        position = chat_answer["position"]
        name = chat_answer["name"]
        try:
            is_current_position = chat_answer["is current position"]
        except KeyError:
            is_current_position = chat_answer.get("is_current_position", None)
        
        if is_current_position in CHAT_FALSE_VALUES:
            is_former_position = True
        elif is_current_position in CHAT_TRUE_VALUES:
            is_former_position = False
        else:
            is_former_position = None

        if not validate_name(name):
            return
        
        is_mentioned_in_content = answer_to_bool(
            ask_chat(
                self.llm_client,
                self.is_mentioned_in_content_str(
                    name, web_content
                )
            )
        )
        
        if not is_mentioned_in_content:
            return
        
        is_position_in_country = all([
            answer_to_bool(
                ask_chat(
                    self.llm_client,
                    self.is_position_in_country_str(
                        name, position, country, web_content
                    )
                )
            ),
            answer_to_bool(
                ask_chat(
                    self.llm_client,
                    self.is_mentioned_as_position_in_country_str(
                        name, position, country, web_content
                    )
                )
            )
        ])
        
        is_position_in_state = sum([
            answer_to_bool(
                ask_chat(
                    self.llm_client,
                    self.is_position_in_state_str(
                        name, position, country, state_name, web_content
                    )
                )
            ),
            answer_to_bool(
                ask_chat(
                    self.llm_client,
                    self.is_position_in_state_type_str(
                        name, position, country, state_type, state_name, web_content
                    )
                )
            ),
            answer_to_bool(
                ask_chat(
                    self.llm_client,
                    self.is_mentioned_as_position_in_state_str(
                        name, position, country, state_name, web_content
                    )
                )
            ),
            answer_to_bool(
                ask_chat(
                    self.llm_client,
                    self.is_mentioned_as_position_in_state_type_str(
                        name, position, country, state_type, state_name, web_content
                    )
                )
            ),
        ]) > 2

        if not (is_position_in_country or is_position_in_state):
            return

        # TODO: fix - former too often
        is_former_position_in_country = any([
            answer_to_bool(
                ask_chat(
                    self.llm_client,
                    self.is_position_in_country_str(
                        name, position, country, web_content, former=True
                    )
                )
            ),
            answer_to_bool(
                ask_chat(
                    self.llm_client,
                    self.is_mentioned_as_position_in_country_str(
                        name, position, country, web_content, former=True
                    )
                )
            )
        ])

        is_former_position_in_state = sum([
            answer_to_bool(
                ask_chat(
                    self.llm_client,
                    self.is_position_in_state_str(
                        name, position, country, state_name, web_content, former=True
                    )
                )
            ),
            answer_to_bool(
                ask_chat(
                    self.llm_client,
                    self.is_position_in_state_type_str(
                        name, position, country, state_type, state_name, web_content, former=True
                    )
                )
            ),
            answer_to_bool(
                ask_chat(
                    self.llm_client,
                    self.is_mentioned_as_position_in_state_str(
                        name, position, country, state_name, web_content, former=True
                    )
                )
            ),
            answer_to_bool(
                ask_chat(
                    self.llm_client,
                    self.is_mentioned_as_position_in_state_type_str(
                        name, position, country, state_type, state_name, web_content, former=True
                    )
                )
            ),
        ]) >= 2

        if is_former_position is None:
            if is_former_position_in_country or is_former_position_in_state:
                position = self.make_position_former(position)
        else:
            if sum([is_former_position_in_country, is_former_position_in_state, is_former_position]) >= 2:
                position = self.make_position_former(position)

        return {
            "name": name,
            "country": country,
            "state_type": state_type,
            "state_name": state_name,
            "group_type": group_type,
            "position": position,
        }

    @staticmethod
    def is_mentioned_in_content_str(
        name: str,
        web_content: str,
    ):
        statement = f'{name} is mantioned in text.'
        
        logger.info(f"Asking if {statement}")
        
        return f"""
Is the statement supported by the text? 

statement: <{statement}>

text:  <{web_content[:12000]}>

Answer yes or no without explanation.
Do not focus on your knowledge, focus on text only.
""".strip()

    @staticmethod
    def is_position_in_country_str(
        name: str,
        position: str,
        country: str,
        web_content: str,
        former: bool = False,
    ):
        statement = f'{name} is {"former " if former else ""}{position} in {country}.',
        
        logger.info(f"Asking if {statement}")
        
        return f"""
Is the statement supported by the text? 

statement: {statement}

text:  <{web_content[:12000]}>

Answer yes or no without explanation.
Do not focus on your knowledge, focus on text only.
""".strip()

    @staticmethod
    def is_mentioned_as_position_in_country_str(
        name: str,
        position: str,
        country: str,
        web_content: str,
        former: bool = False,
    ):
        statement = f'{name} is mentioned as {"former " if former else ""}{position} in {country}.',
        
        logger.info(f"Asking if {statement}")
        
        return f"""
Is the statement supported by the text? 

statement: {statement}

text:  <{web_content[:12000]}>

Answer yes or no without explanation.
Do not focus on your knowledge, focus on text only.
""".strip()

    @staticmethod
    def is_position_in_state_str(
        name: str,
        position: str,
        country: str,
        state_name: str,
        web_content: str,
        former: bool = False,
    ):
        statement = f'{name} is {"former " if former else ""}{position} of {state_name} in {country}.',
        
        logger.info(f"Asking if {statement}")
        
        return f"""
Is the statement supported by the text? 

statement: {statement}

text:  <{web_content[:12000]}>

Answer yes or no without explanation.
Do not focus on your knowledge, focus on text only.
""".strip()

    @staticmethod
    def is_position_in_state_type_str(
        name: str,
        position: str,
        country: str,
        state_type: str,
        state_name: str,
        web_content: str,
        former: bool = False,
    ):
        statement = f'{name} is {"former " if former else ""}{position} of {state_name} {state_type} in {country}.',
        
        logger.info(f"Asking if {statement}")
        
        return f"""
Is the statement supported by the text? 

statement: {statement}

text:  <{web_content[:12000]}>

Answer yes or no without explanation.
Do not focus on your knowledge, focus on text only.
""".strip()

    @staticmethod
    def is_mentioned_as_position_in_state_str(
        name: str,
        position: str,
        country: str,
        state_name: str,
        web_content: str,
        former: bool = False,
    ):
        statement = f'{name} is mentioned as {"former " if former else ""}{position} of {state_name} in {country}.',
        
        logger.info(f"Asking if {statement}")
        
        return f"""
Is the statement supported by the text? 

statement: {statement}

text:  <{web_content[:12000]}>

Answer yes or no without explanation.
Do not focus on your knowledge, focus on text only.
""".strip()

    @staticmethod
    def is_mentioned_as_position_in_state_type_str(
        name: str,
        position: str,
        country: str,
        state_type: str,
        state_name: str,
        web_content: str,
        former: bool = False,
    ):
        statement = f'{name} is mentioned as {"former " if former else ""}{position} of {state_name} {state_type} in {country}.',
        
        logger.info(f"Asking if {statement}")
        
        return f"""
Is the statement supported by the text? 

statement: {statement}

text:  <{web_content[:12000]}>

Answer yes or no without explanation.
Do not focus on your knowledge, focus on text only.
""".strip()

    @staticmethod
    def make_position_former(position: str):
        if position.lower().startswith("former"):
            return position
        
        former_position = "former " + position
        logger.info("Changing position to former", position=position, former_position=former_position)

        return "former " + position
