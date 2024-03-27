from abc import ABC, abstractmethod
from typing import Callable, List
import os
import json
from structlog.stdlib import get_logger as get_raw_logger

from s8er.llm_utils.commons import ask_chat


logger = get_raw_logger(os.path.basename(__file__))


class AbstarctPositionFinder(ABC):
    """
    Abstract method for all PEP position finders.
    """
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__()

        self.llm_client = llm_client
        self.web_search_api_client = web_search_api_client

    @abstractmethod
    def find_positions(self, country: str, **kwargs):
        """
        Method that returns list of positions within one country.

        Args:
            country (str): Country of interest.
        """
        pass


class StandardPositionFinder(AbstarctPositionFinder):
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__(llm_client, web_search_api_client)

    def find_positions(
        self,
        country: str,
        position_groups: List[str] = None,
    ) -> dict:
        """
        Finds positions for list of categories of PEPs

        Args:
            country (str): Country of interest.
            position_groups (List[str], optional): Lit of position categories. Defaults to None.

        Returns:
            dict: Dictionary with positions foer each category.
        """
        logger.info(f"Finding positions for groups: {', '.join(position_groups)}")

        chat_bullet_list_str = "\n    - "

        input_str = f"""
Please name all possible positions that could be held by person for specific groups:
{chat_bullet_list_str.join(position_groups)}

Do this with limiting results to {country} country only.
List positions not people.

Answer in the following json format:
{{"answer-available": <true_or_false>, "answer": [{{"group type": "<group type>", "positions": ["<positions_here>"]}}]}}
If there are no results for a group, use null value.
Try to list every poition possible for each category.
"""

        return json.loads(
            ask_chat(
                cached_response_client=self.llm_client,
                input_str=input_str,
                input_kwargs={"response_format": {"type": "json_object"},}
            )
        )


class StateGovenmentPositionFinder(AbstarctPositionFinder):
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__(llm_client, web_search_api_client)
    
    def find_positions(self, country: str) -> dict:
        """
        Finds positions of PEPs within state level of government.

        Args:
            country (str): Country of interest.

        Returns:
            dict: Dictionary with positions.
        """
        logger.info("Searching for positions within state govenments...")
        
        input_str = f"""
Please name all teritory entities in the state level of government in country of {country}.

Answer in the following json format:
{{"answer-available": <true_or_false>, "answer": [{{"name": "<entity name here>", "person_in_charge": "<title held of person in charge here>","list_of_entities": "[<list of names of this entity type>]"}}]}}
An example of answer to such question for United States of America would be: [{{"name": "State", "person_in_charge": "governor", "list_of_entities": [Alaska, Arkansas, Texas, Californie, ...]}}]
An example of answer to such question for Germany would be: [{{"name": "Land", "person_in_charge": "minister-president", "list_of_entities": [Brandenburg, Bavaria, Schleswig-Holstein, ...]}}]
""".strip()

        return json.loads(
            ask_chat(
                cached_response_client=self.llm_client,
                input_str=input_str,
                input_kwargs={"response_format": {"type": "json_object"},}
            )
        )


class CitiesPositionFinder(AbstarctPositionFinder):
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__(llm_client, web_search_api_client)
    
    def find_positions(self, country: str) -> dict:
        """
        Finds positions of PEPs within country's major cities.

        Args:
            country (str): Country of interest.

        Returns:
            dict: Dictionary with positions.
        """
        logger.info("Searching for positions within major cities...")
        
        input_str = f"""
Please name most important cities in country of {country}.
Take into account its population, geostrategic location and govenment locations.

Answer in the following json format:
{{"answer-available": <true_or_false>, "answer": [{{"name": "<entity name here>", "person_in_charge": "<title held of person in charge here>"}}]}}
""".strip()

        return json.loads(
            ask_chat(
                cached_response_client=self.llm_client,
                input_str=input_str,
                input_kwargs={"response_format": {"type": "json_object"},}
            )
        )


class HeadOfStatePositionFinder(AbstarctPositionFinder):
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__(llm_client, web_search_api_client)
    
    def find_positions(self, country: str) -> dict:
        """
        Finds positions of PEPs within head of state cathegory.

        Args:
            country (str): Country of interest.

        Returns:
            dict: Dictionary with positions.
        """
        logger.info("Searching for head of state positions...")
        
        input_str = f"""
Please find name positions that could be held by person considered a "Head of government or state".

Do this with limiting results to {country} country only.
List positions not people.

Answer in the following json format:
{{"answer-available": <true_or_false>, "answer": [{{"group type": "Head of government or state", "positions": ["<positions_here>"]}}]}}
If there are no results for a group, use null value.
Try to list every poition possible for each category that may be associated with speciffic person.
"""

        return json.loads(
            ask_chat(
                cached_response_client=self.llm_client,
                input_str=input_str,
                input_kwargs={"response_format": {"type": "json_object"},}
            )
        )


class ExecutiveBranchOfGovenmentPositionFinder(AbstarctPositionFinder):
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__(llm_client, web_search_api_client)
    
    def find_positions(self, country: str) -> dict:
        """
        Finds positions of PEPs within executive branch of government category.

        Args:
            country (str): Country of interest.

        Returns:
            dict: Dictionary with positions.
        """
        logger.info("Searching for positions within executive branch of government...")
        
        input_str = f"""
Please name all possible positions that could be held by person for executive branch of government.
Try to list every poition possible for each category. Include all ministers, heads of executive branches of government departments etc.
Do this with limiting results to {country} country only.
List positions not people.

Answer in the following json format:
{{"answer-available": <true_or_false>, "answer": [{{"group type": "Executive branch of government", "positions": ["<positions_here>"]}}]}}
If there are no results for a group, use null value.
"""

        return json.loads(
            ask_chat(
                cached_response_client=self.llm_client,
                input_str=input_str,
                input_kwargs={"response_format": {"type": "json_object"},}
            )
        )


class JudicialBranchOfGovenmentPositionFinder(AbstarctPositionFinder):
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__(llm_client, web_search_api_client)
    
    def find_positions(self, country: str) -> dict:
        """
        Finds positions of PEPs within judical branch of government category.

        Args:
            country (str): Country of interest.

        Returns:
            dict: Dictionary with positions.
        """
        logger.info("Searching for positions within judical branch of government...")
        
        input_str = f"""
Please name all possible positions that could be held by person for judicial branch of government.
Try to list every poition possible for each category. Include all ministers, heads of executive branches of government departments etc.
Do this with limiting results to {country} country only.
List positions not people.

Answer in the following json format:
{{"answer-available": <true_or_false>, "answer": [{{"group type": "Judicial branch of government", "positions": ["<positions_here>"]}}]}}
If there are no results for a group, use null value.
"""

        return json.loads(
            ask_chat(
                cached_response_client=self.llm_client,
                input_str=input_str,
                input_kwargs={"response_format": {"type": "json_object"},}
            )
        )


class LegislativeBranchOfGovenmentPositionFinder(AbstarctPositionFinder):
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__(llm_client, web_search_api_client)
    
    def find_positions(self, country: str) -> dict:
        """
        Finds positions of PEPs within legislative branch of government category.

        Args:
            country (str): Country of interest.

        Returns:
            dict: Dictionary with positions.
        """
        logger.info("Searching for positions within legislative branch of government...")
        
        input_str = f"""
Please name all possible positions that could be held by person for legislative branch of government.
Try to list every poition possible for each category. Include all ministers, heads of executive branches of government departments etc.
Do this with limiting results to {country} country only.
List positions not people.

Answer in the following json format:
{{"answer-available": <true_or_false>, "answer": [{{"group type": "Legislative branch of government", "positions": ["<positions_here>"]}}]}}
If there are no results for a group, use null value.
"""

        return json.loads(
            ask_chat(
                cached_response_client=self.llm_client,
                input_str=input_str,
                input_kwargs={"response_format": {"type": "json_object"},}
            )
        )
