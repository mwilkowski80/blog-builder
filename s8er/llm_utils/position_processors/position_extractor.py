from abc import ABC, abstractmethod
from typing import Callable, List, Dict, Any
import bs4
from typing import Dict, Any
from requests.exceptions import HTTPError, SSLError
import os
import json
from structlog.stdlib import get_logger as get_raw_logger
from htmldate import find_date

from s8er.llm_utils.commons import (
    ask_chat,
    timeout,
    get_url,
    is_answer_available,
    validate_name,
    CHAT_ONE_VALUES,
    CHAT_TRUE_VALUES,
    CHAT_FALSE_VALUES,
    WEB_CONTENT_MAX_LEN,
)
from s8er.llm_utils.halucination_detectors.selfcheck import BaseSelfValidator
from s8er.llm_utils.halucination_detectors.prompt_validation_templates import (
    IsSentenceSuportedByText,
    IsSentenceSuportedByText2,
    IsSentenceSuportedByContext,
    IsSentenceSuportedByContext2
)


logger = get_raw_logger(os.path.basename(__file__))


class AbstractPositionExtractor(ABC):
    """
    Abstract class of all position extractors. Each should search for given list of positions and return list of people found.
    """
    prompt_validation_templates = [
        IsSentenceSuportedByText(),
        # IsSentenceSuportedByText2(),
        IsSentenceSuportedByContext(),
        # IsSentenceSuportedByContext2(),
    ]

    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__()

        self.llm_client = llm_client
        self.web_search_api_client = web_search_api_client
        self.validotor = BaseSelfValidator(
            llm_client=llm_client,
            web_search_api_client=web_search_api_client
        )

    def is_position_unique(self, country: str, position: str) -> bool:
        logger.info("Checking if position is unique...")

        input_str = f"""
How many persons holding such position might there be in the country?
Position: {position}
Country: {country}

Possible answer values: "one", "many".

Answer in the following json format:
{{"answer": <Number of persons>}}
        """.strip()

        position_output = json.loads(
            ask_chat(
                cached_response_client=self.llm_client,
                input_str=input_str,
                input_kwargs={"response_format": {"type": "json_object"},}
            )
        )

        return position_output.get("answer") in CHAT_ONE_VALUES

    def get_position_holders(
        self,
        web_search_str_func_args: list = None,
        web_search_str_func_kwargs: dict = None,
        web_content_to_entities_func_args: list = None,
        web_content_to_entities_func_kwargs: dict = None,
        validate_entities_args: list = None,
        validate_entities_kwargs: dict = None,
    ) -> List[Dict[str, Any]]:
        """
        Method used by each extractor to process one of positions from position finder's ouput.

        Returns:
            List[Dict[str, Any]]: List of entities found and validated.
        """

        if not web_search_str_func_args:
            web_search_str_func_args = []
        if not web_search_str_func_kwargs:
            web_search_str_func_kwargs = {}

        if not web_content_to_entities_func_args:
            web_content_to_entities_func_args = []
        if not web_content_to_entities_func_kwargs:
            web_content_to_entities_func_kwargs = {}
            
        if not validate_entities_args:
            validate_entities_args = []
        if not validate_entities_kwargs:
            validate_entities_kwargs = {}

        
        search_str = self.web_search_str(*web_search_str_func_args, **web_search_str_func_kwargs)

        logger.info("Searching for web content...", search_str=search_str)
        try:
            web_search_results = self.web_search_api_client(search_str)
        except Exception as exc:
            logger.error(
                "Web search failed",
                exc=exc.__class__.__name__,
                msg=str(exc),
            )
            web_search_results = []

        entities_found = []

        for search_result in web_search_results:
            logger.info("Getting url content...", title=search_result["title"] ,url=search_result["href"])
            try:
                with timeout(seconds=30):
                    web_content = bs4.BeautifulSoup(get_url(search_result["href"]), features="lxml").text

                try:
                    web_content_date = find_date(search_result["href"])
                except Exception as exc:
                    logger.warn(
                        "Web content date finding error.",
                        exception=exc.__class__.__name__,
                        msg=str(exc),
                        title=search_result["title"],
                        url=search_result["href"],
                    )
                    web_content_date = None

                entities_meta = {
                    "url": search_result["href"],
                    "web_content_date": web_content_date
                }

                web_source_entities = self.web_content_to_entities(
                        web_content,
                        *web_content_to_entities_func_args,
                        **web_content_to_entities_func_kwargs,
                    ) | entities_meta
                
                web_source_entities = self.validate_entities_from_content(
                    web_content,
                    web_source_entities,
                    *validate_entities_args,
                    **validate_entities_kwargs,
                )

                entities_found.append(web_source_entities)

            except HTTPError as err:
                logger.warn(
                    f"HTTPError",
                    code=err.response.status_code,
                    reason=err.response.reason,
                    title=search_result["title"],
                    url=search_result["href"],
                )

            except SSLError as err:
                logger.warn(
                    "SSLError",
                    title=search_result["title"],
                    url=search_result["href"],
                )

            except Exception as exc:
                logger.error(
                    search_result["title"],
                    exc=exc.__class__.__name__,
                    msg=str(exc),
                    title=search_result["title"],
                    url=search_result["href"]
                )

        return entities_found
    
    @abstractmethod
    def get_entities(self, country: str, position_finder_results: Dict[str, Any]):
        """
        Abstract method to find all PEPs holding positions found with position finder.

        Args:
            country (str): Country of interet
            position_finder_results (Dict[str, Any]): List of positions returned by finder
        """
        pass

    @abstractmethod
    def web_search_str(self, *args, **kwargs) -> str:
        """
        Abstract method for creating web search phrase.

        Returns:
            str: web search phrease - input for web search API
        """
        pass

    @abstractmethod
    def web_content_to_entities(self, web_content: str, *args, **kwargs):
        """
        Abstract method for getting entities form web page.

        Args:
            web_content (str): web text content
        """
        pass

    @abstractmethod
    def validate_entities_from_content(self, web_content: str, entities_found, *args, **kwargs):
        """
        Abstract method for validating entities found in web sources.

        Args:
            web_content (str): web text content
            entities_found (_type_): entities found in web source

        Returns:
            _type_: entities found in web source that passed validation step
        """
        return entities_found

    @abstractmethod
    def _convert_entities_to_unified_format(self, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass
        """
        Abstract method for converting entities to unified format.

        Returns:
            _type_: Entities in unified format.
        """

    @staticmethod
    def _is_answer_available(entities_found: Dict[str, Any]) -> bool:
        if "answer_available" in entities_found:
            answer_available = entities_found["answer_available"]
        elif "answer-available" in entities_found:
            answer_available = entities_found["answer-available"]
        elif "answer available" in entities_found:
            answer_available = entities_found["answer available"]
        else:
            return False

        if (answer_available in CHAT_FALSE_VALUES) or len(entities_found.get("answer", [])) == 0:
            return False

        return True


class StandardPositionExtractor(AbstractPositionExtractor):
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__(llm_client, web_search_api_client)

    def web_search_str(self, country, position) -> str:
        position_unique = self.is_position_unique(country, position)

        if position_unique:
            return f"{position} of {country}"

        return f"list of {position}s in {country}"

    def web_content_to_entities(self, web_content: str, country: str, position: str) -> Dict[str, Any]:
        logger.info("Extracting entities form web content...")
        input_str = f"""
Analyze the content of the page and list who were mentioned as {position} of {country} country.

Answer only if full name and surname was found. Remember that names are capitalized.

Answer in the following json format:
{{"answer-available": <true_or_false>, "answer": [{{"position": "<position_here">, "name": "<name_here>", "is current position": "<true, false or unknown>"}}]}}

Content of web page:
        {web_content[:12000]}
        """.strip()

        chat_output = json.loads(
            ask_chat(
                cached_response_client=self.llm_client,
                input_str=input_str,
                input_kwargs={"response_format": {"type": "json_object"},}
            )
        )

        return chat_output

    def get_entities(self, country: str, position_finder_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        if position_finder_results is None:
            logger.error(f"No positions found for {country} - None value")
            return

        if not is_answer_available(position_finder_results):
            return

        entities_found = []

        for position_group in position_finder_results["answer"]:
            group_type = position_group["group type"]
            positions = position_group["positions"]

            logger.info(f"Processing group: {group_type}")

            if positions is None:
                logger.warn(
                    "No positions returned by chat - use in cht output validator",
                    country=country,
                    position_group=position_group,
                )
                continue

            for position in positions:
                logger.info(f"Processing position: {position}")

                position_entities = self.get_position_holders(
                    web_search_str_func_kwargs={"country": country, "position": position},
                    web_content_to_entities_func_kwargs={"country": country, "position": position},
                    validate_entities_kwargs={"country": country, "position": position, "group_type": group_type},
                )

                entities_found_item = {
                    "country": country,
                    "group_type": group_type,
                    "position": position,
                    "entities_found": position_entities,
                }

                entities_found.append(entities_found_item)

        entities = self._convert_entities_to_unified_format(entities_found)
        return entities

    def _validate_single_entity(
        self,
        web_content: str,
        entity: Dict[str, Any],
        country: str,
        group_type: str,
    ) -> bool:
        # is entity even in source text?
        statement = f'{entity["name"]} is mantioned in text.'
        
        logger.info(
            "Veryfying statement based on text",
            statement=statement,
        )
        
        is_mentioned = self.validotor.validate_statement(
            statement,
            web_content[:WEB_CONTENT_MAX_LEN],
            self.prompt_validation_templates
        )
        
        if not is_mentioned:
            return False
        
        is_position_in_country_statements = [
            f'{entity["name"]} is {entity["position"]} in {country}.',
            f'{entity["name"]} is mentioned as {entity["position"]} in {country}.',
        ]
        
        logger.info(
            f'Veryfying if {entity["name"]} is {entity["position"]} in {country}...'
        )
        
        is_position_in_country = self.validotor.validate_by_multiple_statements(
            is_position_in_country_statements,
            web_content[:WEB_CONTENT_MAX_LEN],
            self.prompt_validation_templates,
            statements_vote_type="halved",
            templates_vote_type="majority"
        )

        if not is_position_in_country:
            return False

        return True

    def validate_entities_from_content(
        self,
        web_content: str,
        entities_found: Dict[str, Any],
        country: str,
        position: str,
        group_type: str,
        **kwargs,
    ) -> Dict[str, Any]:
        if not self._is_answer_available(entities_found):
            return entities_found

        validation_mask = [True for _ in entities_found["answer"]]
        for i, entity in enumerate(entities_found["answer"]):

            if not validate_name(entity["name"]):
                entities_found["answer"][i] = None

            # try: # TODO: could this be accurately used?
            #     is_current_position = entity["is current position"]
            # except KeyError:
            #     is_current_position = entity.get("is_current_position", None)

            verdict = self._validate_single_entity(
                web_content,
                entity,
                country,
                group_type,
            )

            if not verdict:
                logger.info(
                    "Halucination detected",
                    entity_name=entity["name"],
                )

            validation_mask[i] = verdict

        entities_found["answer"] = [entity for entity, verdict in zip(entities_found["answer"], validation_mask) if verdict]
        return entities_found

    def _convert_entities_to_unified_format(self, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        entities_in_unified_format = []
        for entity_group in entities:
            for entities_found in entity_group["entities_found"]:
                for entity in entities_found.get("answer", []):
                    if entity is None:
                        continue

                    if not isinstance(entity, dict):
                        logger.error(
                            "Invalid entity type",
                            type=type(entity),
                            entity=entity,
                        )
                        continue

                    unfied_entity = {
                        "searched_position": entity_group.get("position"),
                        "searched_topic": entity_group.get("group_type"),
                        "source": {
                            "url": entities_found["url"],
                            "date": entities_found["web_content_date"],
                        },
                        "properties": {
                            "name": entity["name"],
                            "position": entity["position"],
                            "country": entity_group["country"],
                        },
                    }
                    logger.info("Created entity", **unfied_entity["properties"])
                    entities_in_unified_format.append(unfied_entity)

        return entities_in_unified_format


class StateGovenmentPositionExtractor(AbstractPositionExtractor):
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__(llm_client, web_search_api_client)
    
    def web_search_str(
        self,
        country: str,
        person_in_charge: str,
        state_type_name: str,
        state_name: str
    ) -> str:
        return f"{person_in_charge} of {state_name} {state_type_name} in country of {country}"

    def web_content_to_entities(
        self,
        web_content: str,
        country: str,
        person_in_charge: str,
        state_type_name: str,
        state_name: str,
    ) -> Dict[str, Any]:
        logger.info("Extracting entities form web content...")
        input_str = f"""
Analyze the content of the page and who was mentioned as {person_in_charge} of {state_name} {state_type_name} in country of {country}.
Answer only if full name and surname was found. Remember that names are capitalized.

Answer in the following json format:
{{"answer-available": <true_or_false>, "answer": [{{"position": "<position_here">, "name": "<name_here>", "is current position": "<true, false or unknown>"}}]}}

Content of web page:
{web_content[:12000]}
        """.strip()

        return json.loads(
            ask_chat(
                cached_response_client=self.llm_client,
                input_str=input_str,
                input_kwargs={"response_format": {"type": "json_object"},}
            )
        )

    def get_entities(self, country: str, position_finder_results: Dict[str, Any]) -> Dict[str, Any]:
        if position_finder_results is None:
            logger.error(f"No positions found for {country} - None value")
            return
        
        if not is_answer_available(position_finder_results):
            logger.error(f"No positions found for {country} - answer not available")
            return

        entities_found = []

        for state_type_dict in position_finder_results["answer"]:
            state_type_name = state_type_dict["name"]
            person_in_charge = state_type_dict["person_in_charge"]

            logger.info(f"Processing: {person_in_charge} of {state_type_name}...")

            for state_name in state_type_dict["list_of_entities"]:
                logger.info(f"Processing state: {state_name}...")

                state_position_holders = self.get_position_holders(
                    web_search_str_func_kwargs={
                        "country": country,
                        "person_in_charge": person_in_charge,
                        "state_type_name": state_type_name,
                        "state_name": state_name
                    },
                    web_content_to_entities_func_kwargs={
                        "country": country,
                        "person_in_charge": person_in_charge,
                        "state_type_name": state_type_name,
                        "state_name": state_name
                    },
                    validate_entities_kwargs={
                        "country": country,
                        "group_type": "State government",
                        # "person_in_charge": person_in_charge,
                        "state_type_name": state_type_name,
                        "state_name": state_name
                    },
                )

                entities_found_item = {
                    "country": country,
                    "group_type": "State government",
                    "position": person_in_charge,
                    "state_type": state_type_name,
                    "state_name": state_name,
                    "entities_found": state_position_holders,
                }

                entities_found.append(entities_found_item)

        entities = self._convert_entities_to_unified_format(entities_found)
        return entities

    def _validate_single_entity(
        self,
        web_content: str,
        entity: Dict[str, Any],
        country: str,
        state_type_name: str,
        group_type: str,
        state_name: str,
    ) -> bool:
        # is entity even in source text?
        statement = f'{entity["name"]} is mantioned in text.'
        
        logger.info(
            "Veryfying statement based on text",
            statement=statement,
        )
        
        is_mentioned = self.validotor.validate_statement(
            statement,
            web_content[:WEB_CONTENT_MAX_LEN],
            self.prompt_validation_templates
        )
        
        if not is_mentioned:
            logger.info(
                "Halucination detected - person not in text",
                entity_name=entity["name"],
            )
            return False
        
        is_position_in_country_statements = [
            f'{entity["name"]} is {entity["position"]} in {country}.',
            f'{entity["name"]} is mentioned as {entity["position"]} in {country}.',
        ]
        
        logger.info(
            f'Veryfying if {entity["name"]} is {entity["position"]} in {country}...'
        )
        
        is_position_in_country = self.validotor.validate_by_multiple_statements(
            is_position_in_country_statements,
            web_content[:WEB_CONTENT_MAX_LEN],
            self.prompt_validation_templates,
            statements_vote_type="halved",
            templates_vote_type="majority"
        )

        if not is_position_in_country:
            logger.info(
                "Halucination detected - not a position holder in country",
                entity_name=entity["name"],
            )
            return False
        
        logger.info(
            f'Veryfying if {entity["name"]} is {entity["position"]} of {state_name} {state_type_name} in {country}...'
        )

        is_position_in_state_statements = [
            f'{entity["name"]} is {entity["position"]} of {state_name} in {country}.',
            f'{entity["name"]} is {entity["position"]} of {state_name} {state_type_name} in {country}.',
            f'{entity["name"]} is mentioned as {entity["position"]} of {state_name} in {country}.',
            f'{entity["name"]} is mentioned as {entity["position"]} of {state_name} {state_type_name} in {country}.',
        ]
        is_position_in_state = self.validotor.validate_by_multiple_statements(
            is_position_in_state_statements,
            web_content[:WEB_CONTENT_MAX_LEN],
            self.prompt_validation_templates,
            statements_vote_type="halved",
            templates_vote_type="majority"
        )
        
        if not is_position_in_state:
            logger.info(
                "Halucination detected - not a position holder in state",
                entity_name=entity["name"],
            )
            return False
        
        return True

    def validate_entities_from_content(
        self,
        web_content: str,
        entities_found: Dict[str, Any],
        country: str,
        state_type_name: str,
        group_type: str,
        state_name: str,
        **kwargs,
    ) -> Dict[str, Any]:
        if not self._is_answer_available(entities_found):
            return entities_found

        validation_mask = [True for _ in entities_found["answer"]]
        for i, entity in enumerate(entities_found["answer"]):
            
            if not validate_name(entity["name"]):
                entities_found["answer"][i] = None

            verdict = self._validate_single_entity(
                web_content,
                entity,
                country,
                state_type_name,
                group_type,
                state_name
            )

            validation_mask[i] = verdict

        entities_found["answer"] = [entity for entity, verdict in zip(entities_found["answer"], validation_mask) if verdict]
        return entities_found

    def _convert_entities_to_unified_format(self, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        entities_in_unified_format = []
        for entity_group in entities:
            for entities_found in entity_group["entities_found"]:
                for entity in entities_found.get("answer", []):
                    if entity is None:
                        continue

                    if not isinstance(entity, dict):
                        logger.error(
                            "Invalid entity type",
                            type=type(entity),
                            entity=entity,
                        )
                        continue

                    unfied_entity = {
                        "searched_position": f'{entity_group.get("position")} of {entity_group.get("state_name")} {entity_group.get("state_type")}',
                        "searched_topic": entity_group.get("group_type"),
                        "source": {
                            "url": entities_found["url"],
                            "date": entities_found["web_content_date"],
                        },
                        "properties": {
                            "name": entity["name"],
                            "position": entity["position"],
                            "country": entity_group["country"],
                        }
                    }
                    logger.info("Created entity", **unfied_entity["properties"])
                    entities_in_unified_format.append(unfied_entity)

        return entities_in_unified_format


class CitiesPositionExtractor(StateGovenmentPositionExtractor):
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__(llm_client, web_search_api_client)

    def get_entities(self, country: str, position_finder_results: Dict[str, Any]) -> Dict[str, Any]:
        if position_finder_results is None:
            logger.error(f"No positions found for {country} - None value")
            return

        if not is_answer_available(position_finder_results):
            return

        entities_found = []

        for city in position_finder_results["answer"]:
            city_name = city["name"]
            person_in_charge = city["person_in_charge"]

            logger.info(f"Processing: {person_in_charge} of {city_name}...")

            state_position_holders = self.get_position_holders(
                web_search_str_func_kwargs={
                        "country": country,
                        "person_in_charge": person_in_charge,
                        "state_type_name": "city",
                        "state_name": city_name
                    },
                web_content_to_entities_func_kwargs={
                        "country": country,
                        "person_in_charge": person_in_charge,
                        "state_type_name": "city",
                        "state_name": city_name
                    },
                validate_entities_kwargs={
                    "country": country,
                    "group_type": "State government",
                    # "person_in_charge": person_in_charge,
                    "state_type_name": "city",
                    "state_name": city_name
                },
            )

            entities_found_item = {
                "country": country,
                "group_type": "Major cities",
                "position": person_in_charge,
                "state_type": "City",
                "state_name": city_name,
                "entities_found": state_position_holders,
            }

            entities_found.append(entities_found_item)

        entities = self._convert_entities_to_unified_format(entities_found)
        return entities
