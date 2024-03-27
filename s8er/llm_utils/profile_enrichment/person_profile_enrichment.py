from typing import Callable, List, Dict, Any, Union
import bs4
from typing import Dict, Any
from requests.exceptions import HTTPError, SSLError
import os
import json
from structlog.stdlib import get_logger as get_raw_logger
from htmldate import find_date
from itertools import chain, product
from strsimpy.normalized_levenshtein import NormalizedLevenshtein
import pandas as pd
from pandas._libs.tslibs.parsing import DateParseError
from i18naddress import InvalidAddressError, normalize_address
from time import sleep
from collections import Counter
import re


from s8er.llm_utils.commons import (
    ask_chat,
    timeout,
    get_url,
    validate_name,
    merge_dicts,
    CHAT_UNKNOWN_STR_LOWER_VALUES,
    WEB_CONTENT_MAX_LEN,
)
from s8er.llm_utils.halucination_detectors.selfcheck import BaseSelfValidator
from s8er.llm_utils.halucination_detectors.prompt_validation_templates import (
    IsSentenceSuportedByText,
    IsSentenceSuportedByText2,
    IsSentenceSuportedByContext,
    IsSentenceSuportedByContext2,
    ProveStatementWithText,
    ProveStatementWithText2,
    ProveStatementWithText3,
)
from s8er.llm_utils.profile_enrichment.profile_enrichment import AbstractProfileEnricher
from s8er.llm_utils.profile_enrichment.queries import (
    ASK_FOR_PROPERTIES_INPUT_STRINGS,
    ASK_FOR_POSITIONS_HELD_INPUT_STRINGS,
    ASK_FOR_ASSOCIATES_INPUT_STRINGS,
    ASK_FOR_ORGANIZATIONS_INPUT_STRINGS,
    ASK_FOR_RELATIVES_INPUT_STRINGS,
    ASK_FOR_ADDRESSES_INPUT_STRINGS,
)


logger = get_raw_logger(os.path.basename(__file__))


TREAT_EMPTY_NAME_PARTIALS_AS_VALID = True
NAME_SIMILARITY_THRESHOLD = 0.4


class PersonProfileEnricher(AbstractProfileEnricher):
    prompt_validation_templates = [
        IsSentenceSuportedByText(),
        # IsSentenceSuportedByText2(),
        IsSentenceSuportedByContext(),
        # IsSentenceSuportedByContext2(),
    ]
    
    prompt_proof_templates = [
        ProveStatementWithText(),
        ProveStatementWithText2(),
        ProveStatementWithText3(),
    ]

    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        super().__init__(llm_client, web_search_api_client)

    def enrich_entities(self, entities: List[Dict[str, Any]], savepath: str = None, savemode="a") -> List[Dict[str, Any]]:
        entities_enriched = entities.copy()
        for i, entity in enumerate(entities_enriched):
            logger.info(f"Enriching profile {i+1}/{len(entities)}...", name=entity["properties"]["name"])

            # if len(entity["web_search_properties"]) == 0:
            #     entities_enriched[i] = self.enrich_entity(entity)
            # else:
            #     entities_enriched[i] = entity
            #     logger.info("Spipping entity", name=entity["properties"]["name"])

            entities_enriched[i] = self.enrich_entity(entity)

            self.prepare_enrichment_quality_log(entity)
            self.postprocess_entity(entity)

            logger.info(f"Saving raw profile...", name=entity["properties"]["name"], savepath=savepath)  

            if savepath:
                if savemode == "w" and i == 0:
                    _savemode = "w"
                else:
                    _savemode = "a"

                with open(savepath, _savemode, encoding="utf-8") as f:
                    json.dump(entity, f)
                    f.write("\n") 

            entity["web_search_properties_source_distinct"] = self.merge_raw_web_search_properties(entity)
            entity["web_search_properties_consolidated"] = self.consolidate_distinct_properties(entity)

            logger.info(f"Saving profile...", name=entity["properties"]["name"], savepath=savepath)  

            if savepath:
                if savemode == "w" and i == 0:
                    _savemode = "w"
                else:
                    _savemode = "a"

                with open(savepath.replace(".jsonl", "_source_distinct.jsonl"), _savemode, encoding="utf-8") as f:
                    json.dump(
                        ({"id": entity["id"]} if "id" in entity else {}) | \
                        ({"searched_position": entity["searched_position"]} if "searched_position" in entity else {}) | {
                            "properties": entity["properties"],
                            "web_search_properties": entity["web_search_properties"],
                            "web_search_properties_source_distinct": entity["web_search_properties_source_distinct"],
                        },
                        f
                    )
                    f.write("\n")

                with open(savepath.replace(".jsonl", "_properties_consolidated.jsonl"), _savemode, encoding="utf-8") as f:
                    json.dump(
                        ({"id": entity["id"]} if "id" in entity else {}) | \
                        ({"searched_position": entity["searched_position"]} if "searched_position" in entity else {}) | {
                            "properties": entity["properties"],
                            "web_search_properties": entity["web_search_properties"],
                            "web_search_properties_consolidated": entity["web_search_properties_consolidated"],
                        },
                        f
                    )
                    f.write("\n")  

        return entities_enriched

    def enrich_entity(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        web_search_str_list = self._get_entity_web_search_strings(entity)
        logger.info("Searching for web content...", web_search_str_list=web_search_str_list)

        properties_found = []

        # try: 001
        
        web_search_results = []
        for search_str in web_search_str_list:
            try:
                web_search_results.extend(self.web_search_api_client(search_str))
                sleep(1)
            except Exception as exc:
                logger.error(
                    "Web search failed",
                    exc=exc.__class__.__name__,
                    msg=str(exc),
                )

        for i, result in enumerate(web_search_results):
            if "href" not in result:
                web_search_results[i]["href"] = result["url"]

        web_search_results = {result["href"]: result for result in web_search_results}.values()
        
        logger.info("Web sources found", web_search_sources=[result["href"] for result in web_search_results])

        for web_search_num, search_result in enumerate(web_search_results):
            logger.info(
                f"Getting url content {web_search_num + 1}/{len(web_search_results)}...",
                title=search_result["title"],
                url=search_result["href"]
            )
            try:
                with timeout(seconds=30):
                    web_content = bs4.BeautifulSoup(get_url(search_result["href"]), features="lxml").text

                def clean_web_source(text):
                    return re.sub(r'\s(?=\s)','',re.sub(r'\s',' ', text))
                
                web_content = clean_web_source(web_content)

                if len(web_content) == 0:
                    logger.warn(
                        "Empty web content",
                        title=search_result["title"],
                        url=search_result["href"],
                    )
                    continue

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

                properties_meta = {
                    "url": search_result["href"],
                    "date": web_content_date,
                    # "web_content": web_content,
                }

                web_source_properties = self.get_entity_properties_from_source(
                    web_content,
                    entity
                )
                web_source_properties["source"] = properties_meta

                if len(web_source_properties) > 0:
                    web_source_properties = self.validate_entity_web_source_properties(
                        web_content,
                        entity,
                        web_source_properties
                    )

                    properties_found.append(web_source_properties)

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

        web_search_properties = {}
        if len(properties_found) > 0:
            web_search_properties = self._merge_web_source_properties(properties_found)  

        entity["web_search_properties"] = web_search_properties
        entity["web_search_properties_raw"] = properties_found

        # except Exception as exc: 001
        #     logger.warn(
        #         "Entity enrichment failed.",
        #         exception=exc.__class__.__name__,
        #         msg=str(exc),
        #         title=search_result["title"],
        #         url=search_result["href"],
        #     )
        #     entity["web_search_properties"] = {}
        return entity

    def get_entity_properties_from_source(
        self,
        web_content: str,
        entity: Dict[str, Any],
    ) -> Dict[str, Any]:
        
        return {
            "properties": self._ask_for_entity_properties_category(web_content, entity, "properties"),
            "positions_held": self._ask_for_entity_properties_category(web_content, entity, "positions_held"),
            "associates": self._ask_for_entity_properties_category(web_content, entity, "associates"),
            "relatives": self._ask_for_entity_properties_category(web_content, entity, "relatives"),
            "organizations": self._ask_for_entity_properties_category(web_content, entity, "organizations"),
            "addresses": self._ask_for_entity_properties_category(web_content, entity, "addresses"),
        }

    def prepare_enrichment_quality_log(self, entity: Dict[str, Any]):
        logger.info(
            "Conducting quality check of enrichment data..."
        )
        entity["web_search_properties"]["quality"] = {
            "quality_log": [],
            "quality_standard": True
        }
        if len(entity["web_search_properties"]) == 1:
            logger.warn("Not enriched entity")
            entity["web_search_properties"]["quality"]["quality_log"].append("Lack of enrichment properties")
            entity["web_search_properties"]["quality"]["quality_standard"] = False
            return

        if not self._prepare_quality_log_of_name_partials(entity):
            logger.warn("Invalid name partials")
            entity["web_search_properties"]["quality"]["quality_log"].append("Mismatch in name partials")
            entity["web_search_properties"]["quality"]["quality_standard"] = False

    def validate_entity_web_source_properties(
        self,
        web_content: str,
        entity: Dict[str, Any],
        web_source_properties: Dict[str, Any],
    ) -> Dict[str, Any]:

        web_source_properties["proofs"] = {}

        logger.info("Validating entity personal details...")
        web_source_properties["properties"], web_source_properties["proofs"]["properties"] = self._validate_properties(
            web_content,
            web_source_properties["source"],
            entity,
            web_source_properties["properties"],
        )

        logger.info("Validating entity positions held...")
        web_source_properties["positions_held"], web_source_properties["proofs"]["positions_held"] = self._validate_positions_held(
            web_content,
            web_source_properties["source"],
            entity,
            web_source_properties["positions_held"],
        )

        logger.info("Validating entity associates list...")
        web_source_properties["associates"], web_source_properties["proofs"]["associates"] = self._validate_associates(
            web_content,
            web_source_properties["source"],
            entity,
            web_source_properties["associates"],
        )

        logger.info("Validating entity relatives list...")
        web_source_properties["relatives"], web_source_properties["proofs"]["relatives"] = self._validate_relatives(
            web_content,
            web_source_properties["source"],
            entity,
            web_source_properties["relatives"]
        )

        logger.info("Validating entity organizations...")
        web_source_properties["organizations"], web_source_properties["proofs"]["organizations"] = self._validate_organizations(
            web_content,
            web_source_properties["source"],
            entity,
            web_source_properties["organizations"]
        )

        logger.info("Validating entity addresses...")
        web_source_properties["addresses"], web_source_properties["proofs"]["addresses"] = self._validate_addresses(
            web_content,
            web_source_properties["source"],
            entity,
            web_source_properties["addresses"]
        )

        return web_source_properties

    def postprocess_entity(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        entity = self._postprocess_properties(entity)
        entity = self._postprocess_positions_held(entity)
        entity = self._postprocess_associates(entity)
        entity = self._postprocess_relatives(entity)
        entity = self._postprocess_organizations(entity)
        entity = self._postprocess_addresses(entity)

        return entity

    def merge_raw_web_search_properties(self, entity: Dict[str, Any]) -> Dict[str, List[Any]]:
        logger.info("Merging raw web search properties...")
        
        web_properties = {
            "properties": [],
            "positions_held": [],
            "associates": [],
            "relatives": [],
            "organizations": [],
            "addresses": [],
        }
        for properties_from_one_source in entity["web_search_properties_raw"]:
            source_meta = properties_from_one_source["source"]
            
            properties = {
                prop: {"values": details} | {
                    "proofs": properties_from_one_source["proofs"]["properties"][prop]
                } | {
                    "source": source_meta
                } for prop, details in properties_from_one_source["properties"].items()
            }
            web_properties["properties"].append(
                properties
            )
            
            positions_held = {
                position: details | {
                    "proofs": properties_from_one_source["proofs"]["positions_held"][position]
                } | {
                    "source": source_meta
                } for position, details in properties_from_one_source["positions_held"].items()
            }
            web_properties["positions_held"].append(
                positions_held
            )
            
            associates = {
                associate: {"values": associate} | {
                    "proofs": properties_from_one_source["proofs"]["associates"][i]
                } | {
                    "source": source_meta
                } for i, associate in enumerate(properties_from_one_source["associates"])
            }
            web_properties["associates"].append(
                associates
            )
            
            relatives = {
                prop: {"values": details} | {
                    "proofs": properties_from_one_source["proofs"]["relatives"][prop]
                } | {
                    "source": source_meta
                } for prop, details in properties_from_one_source["relatives"].items()
            }
            web_properties["relatives"].append(
                relatives
            )
            
            organizations = [
                dict(**organization) | {
                    "proofs": properties_from_one_source["proofs"]["organizations"][i]
                } | {
                    "source": source_meta
                } for i, organization in enumerate(properties_from_one_source["organizations"])
            ]
            web_properties["organizations"].append(
                organizations
            )
            
            addresses = [
                dict(**address) | {
                    "proofs": properties_from_one_source["proofs"]["addresses"][i]
                } | {
                    "source": source_meta
                } for i, address in enumerate(properties_from_one_source["addresses"])
            ]
            web_properties["addresses"].append(
                addresses
            )

        return web_properties

    def consolidate_distinct_properties(self, entity: Dict[str, Any]) -> Dict[str, List[Any]]:
        if "web_search_properties_source_distinct" not in entity:
            web_search_properties_source_distinct = self.merge_raw_web_search_properties(entity)
        else:
            web_search_properties_source_distinct = entity["web_search_properties_source_distinct"]
        
        logger.info("Collecting trustworthy results...")
        web_search_properties_consolidated = {}
        
        # consolidate_properties
        properties = [
            "gender",
            "title",
            "first_name",
            "middle_name",
            "last_name",
            "alias",
            "date_of_birth",
            "date_of_death",
            "residence_country",
            "nalionality",
            "id_numbers"
        ]
        threshold = 2

        consolidated_properties = {}

        for entity_property in properties:
            
            property_values = [
                property_values.get(entity_property) for property_values in web_search_properties_source_distinct["properties"]
            ]
            c = Counter(
                chain.from_iterable([prop_dict.get("values") for prop_dict in property_values if isinstance(prop_dict, dict)])
            )

            all_values = list(c.keys())
            trustworthy_values = [key for key, value in c.items() if value >= threshold]

            if len(all_values) == 0:
                continue
            
            consolidated_properties[entity_property] = []
            for property_value in all_values:
                consolidated_properties[entity_property].append(
                    {
                        "value": property_value,
                        "trusstworhy": property_value in trustworthy_values,
                        "sources": [
                            prop_dict.get("source") | {"proofs": prop_dict.get("proofs")} for prop_dict in property_values \
                                if isinstance(prop_dict, dict) and property_value in prop_dict.get("values")
                        ]
                    }
                )
        web_search_properties_consolidated["properties"] = consolidated_properties
        
        # organizations
        organizations_list = list(chain.from_iterable([
            [
                organization for organization in organizations_list \
                    if isinstance(organization, dict)
            ] for organizations_list in web_search_properties_source_distinct["organizations"]
        ]))
        
        def normalize(org_name):
            return "".join([char for char in org_name if char.isalnum()]).lower()
        
        organizations_combined = {}
        for organization in organizations_list:
            org_name = organization["organization_name"]
            try:
                organizations_combined[normalize(org_name)].append(
                    organization
                )
            except KeyError:
                organizations_combined[normalize(org_name)] = [organization]
        web_search_properties_consolidated["organizations"] = list(organizations_combined.values())
        
        # relatives
        relative_categories = set(
            chain.from_iterable(
                [
                    relatives.keys() for relatives in 
                        web_search_properties_source_distinct["relatives"]
                ]
            )
        )

        consolidated_relatives = {}

        for entity_relatives in relative_categories:
            relative_values = [
                property_values.get(entity_relatives) for property_values in web_search_properties_source_distinct["relatives"]
            ]
            c = Counter(
                chain.from_iterable([rel_dict.get("values") for rel_dict in relative_values if isinstance(rel_dict, dict)])
            )

            all_values = list(c.keys())
            trustworthy_values = [key for key, value in c.items() if value >= threshold]

            if len(all_values) == 0:
                continue

            consolidated_relatives[entity_relatives] = []
            for property_value in all_values:
                consolidated_relatives[entity_relatives].append(
                    {
                        "value": property_value,
                        "trusstworhy": property_value in trustworthy_values,
                        "sources": [
                            prop_dict.get("source") | {"proofs": prop_dict.get("proofs")} for prop_dict in relative_values \
                                if isinstance(prop_dict, dict) and property_value in prop_dict.get("values")
                        ]
                    }
                )
        web_search_properties_consolidated["relatives"] = consolidated_relatives

        # associates
        associate_list = list(
            chain.from_iterable(
                [
                    associates_dict.values() for associates_dict in web_search_properties_source_distinct["associates"]
                ]
            )
        )

        associatess_combined = {}
        for associate in associate_list:
            if "values" in associate:
                associate_name = associate["values"]
                associate["associate_name"] = associate_name
                del associate["values"]
            else:
                associate_name = associate["associate_name"]

            try:
                associatess_combined[normalize(associate_name)].append(
                    associate
                )
            except KeyError:
                associatess_combined[normalize(associate_name)] = [associate]
        web_search_properties_consolidated["associates"] = list(associatess_combined.values())

        # positions held
        web_search_properties_consolidated["positions_held"] = web_search_properties_source_distinct["positions_held"]

        #addresses
        web_search_properties_consolidated["addresses"] = web_search_properties_source_distinct["addresses"]
        
        return web_search_properties_consolidated

    def _ask_for_entity_properties_category(
        self,
        web_content: str,
        entity: Dict[str, Any],
        category: str,
    ) -> Dict[str, Union[str, list]]:

        match category:
            case "properties":
                input_str_func_list = ASK_FOR_PROPERTIES_INPUT_STRINGS
            case "positions_held":
                input_str_func_list = ASK_FOR_POSITIONS_HELD_INPUT_STRINGS
            case "associates":
                input_str_func_list = ASK_FOR_ASSOCIATES_INPUT_STRINGS
            case "relatives":
                input_str_func_list = ASK_FOR_RELATIVES_INPUT_STRINGS
            case "organizations":
                input_str_func_list = ASK_FOR_ORGANIZATIONS_INPUT_STRINGS
            case "addresses":
                input_str_func_list = ASK_FOR_ADDRESSES_INPUT_STRINGS
            case _:
                logger.error(
                    "Unknown entity property group",
                    category=category,
                )

        logger.info(f"Asking for entity {category}")

        chat_response_dict_list = []
        for input_str_func in input_str_func_list:
            try:
                chat_response_dict_list.append(
                    json.loads(
                        ask_chat(
                            cached_response_client=self.llm_client,
                            input_str=input_str_func(entity, web_content),
                            input_kwargs={"response_format": {"type": "json_object"},}
                        )
                    )
                )
            except Exception as exc:
                logger.error(
                        "Finding entity's properties failed",
                        exc=exc.__class__.__name__,
                        msg=str(exc),
                    )

        return chat_response_dict_list
        # if len(chat_response_dict_list) == 0:
        #     return {}
        
        # if len(chat_response_dict_list) == 1:
        #     return merge_dicts(chat_response_dict_list[0])
        
        # return merge_dicts(*chat_response_dict_list)

    def _ask_for_entity_properties(
        self,
        web_content: str,
        entity: Dict[str, Any]
    ) -> Dict[str, Union[str, list]]:
        try:
            return json.loads(
                ask_chat(
                    cached_response_client=self.llm_client,
                    input_str=f"""based on text: <{web_content[:12000]}>
extract information regarding {entity["properties"]["name"]}

Include information such as: ["gender", "title", "first_name", "middle_name", "last_name", "alias", "date_of_birth", "date_of_death", "residence_country", "nalionality", "id_numbers"]
Answer in the following json format:
{{"<property_name>": <property_values>}}

If value is unknown use null value.
""",
                    input_kwargs={"response_format": {"type": "json_object"},}
                )
            )
        except Exception as exc:
            logger.error(
                    "Finding entity's properties failed",
                    exc=exc.__class__.__name__,
                    msg=str(exc),
                )
            return {}

    def _ask_for_entity_positions_held(
        self,
        web_content: str,
        entity: Dict[str, Any]
    ) -> Dict[str, Union[str, list]]:
        try:
            return json.loads(
                ask_chat(
                    cached_response_client=self.llm_client,
                    input_str=f"""based on text: <{web_content[:12000]}>
extract information regarding positions held by {entity["properties"]["name"]}

Answer in the following json format:
{{"<position_held>": {{"start_date": "<start_date_value>", "end_date": "<end_date_vlues>"}}}}

If value is unknown use null value. Use "%Y-%m-%d" datetime string format.
""",
                    input_kwargs={"response_format": {"type": "json_object"},}
                )
            )
        except Exception as exc:
            logger.error(
                    "Finding entity's positions failed",
                    exc=exc.__class__.__name__,
                    msg=str(exc),
                )
            return {}

    def _ask_for_entity_associates(
        self,
        web_content: str,
        entity: Dict[str, Any]
    ) -> Dict[str, Union[str, list]]:
        try:
            return json.loads(
                ask_chat(
                    cached_response_client=self.llm_client,
                    input_str=f"""based on text: <{web_content[:12000]}>
extract information regarding associates of {entity["properties"]["name"]}

Answer in the following json format:
{{"associates": <list of associates names>}}
""",
                    input_kwargs={"response_format": {"type": "json_object"},}
                )
            )
        except Exception as exc:
            logger.error(
                    "Finding entity's associates failed",
                    exc=exc.__class__.__name__,
                    msg=str(exc),
                )
            return {}
        
    def _ask_for_entity_organizations(
        self,
        web_content: str,
        entity: Dict[str, Any]
    ):
        input_str = f"""
based od text: <{web_content[:12000]}>
extract information about organizations D{entity["properties"]["name"]} might be associated with.
Include organization types: "political party", "public organization", "state organization", "multinational organization", "company".
Possible involvement types: founder, member, affiliated, president, other

Answer in the following json format:
{{"organizations": [
    {{
        organization_name: "<organization_name>>",
        organization_type: "<organization_type>",
        involvement_type: "<{entity["properties"]["name"]} involvement in organization>",
        date_start: "<involvement_start_date>",
        date_end: "<involvement_end_date>",
    }}
]}}
Use "%Y-%m-%d" datetime string format.
"""
        try:
            return json.loads(
                ask_chat(
                    cached_response_client=self.llm_client,
                    input_str=input_str,
                    input_kwargs={"response_format": {"type": "json_object"},}
                )
            )
        except Exception as exc:
            logger.error(
                    "Finding entity's ralatives failed",
                    exc=exc.__class__.__name__,
                    msg=str(exc),
                )
            return {}

    def _ask_for_entity_relatives(
        self,
        web_content: str,
        entity: Dict[str, Any]
    ) -> Dict[str, Union[str, list]]:
        try:
            return json.loads(
                ask_chat(
                    cached_response_client=self.llm_client,
                    input_str=f"""based on text: <{web_content[:12000]}>
extract information regarding family relatives of {entity["properties"]["name"]}

Answer in the following json format:
{{"<relationship_name>": <list of related persons>}}
Anser only when full name of the related person is known.
""",
                    input_kwargs={"response_format": {"type": "json_object"},}
                )
            )
        except Exception as exc:
            logger.error(
                    "Finding entity's ralatives failed",
                    exc=exc.__class__.__name__,
                    msg=str(exc),
                )
            return {}

    def _ask_for_entity_addresses(
        self,
        web_content: str,
        entity: Dict[str, Any]
    ) -> Dict[str, Union[str, list]]:
        try:
            return json.loads(
                ask_chat(
                    cached_response_client=self.llm_client,
                    input_str=f"""based on text: <{web_content[:12000]}>
extract information regarding addresses of {entity["properties"]["name"]}.
Extract address details and address type, possible address types are: "personal address", "job address"

Answer in the following json format:
{{"<addresses>": [{{
    "address_type": <address_type>,
    "country_code": <alpha2 country code>,
    "postal_code": <postal code>,
    "country_area", <country area>,
    "city": <city>,
    "street": <street>,
    "number": <number>
}}]}}
Use none values to fill unknown details.
""",
                    input_kwargs={"response_format": {"type": "json_object"},}
                )
            )
        except Exception as exc:
            logger.error(
                    "Finding entity's ralatives failed",
                    exc=exc.__class__.__name__,
                    msg=str(exc),
                )
            return {}

    def _prepare_quality_log_of_name_partials(self, entity: Dict[str, Any]) -> bool:
        """
        LLM profile extraction from time to time fills profile name with values like "Prime Minister", "Governor" etc.
        Profile enrichment has no troubles with enriching such profiles, but does it with details of all persons holding such position.
        Such profiles can be easily filtered by matching searched name with found name partials.
        
        Current implementation finds common parts within names to match profile name. If not found, computes name similarity.
        At least one name combination with similarity above threshold allows profile to be treaten as valid.

        Args:
            entity (Dict[str, Any]): Eriched entity profile

        Returns:
            bool: True if entity should be considered as valid by name
        """
        logger.info("Validating name partials...")
        
        search_name = entity["properties"]["name"].lower()
        name_partials = [
            entity["web_search_properties"]["properties"].get("first_name"),
            entity["web_search_properties"]["properties"].get("middle_name"),
            entity["web_search_properties"]["properties"].get("last_name"),
        ]
        name_partials_reordered = [
            entity["web_search_properties"]["properties"].get("first_name"),
            entity["web_search_properties"]["properties"].get("last_name"),
            entity["web_search_properties"]["properties"].get("middle_name"),
        ]

        names_combined = (
            list(product(
                *[name for name in name_partials if isinstance(name, list)]
            )) + \
            list(product(
                *[name for name in name_partials_reordered if isinstance(name, list)]
            ))
        )
        names_combined = [
            " ".join(name).lower() for name in names_combined
        ]
        names_combined = list({name for name in names_combined if len(name) > 0})
        
        if len(name_partials) == 0 and TREAT_EMPTY_NAME_PARTIALS_AS_VALID:
            logger.info(
                "Empty name partials - treating as valid"
            )
            return True

        if len(name_partials) == 0 and not TREAT_EMPTY_NAME_PARTIALS_AS_VALID:
            logger.info(
                "Empty name partials - treating as invalid"
            )
            return False

        searched_name_in_combined = any([search_name in name for name in names_combined])
        combined_name_in_searched = any([name in search_name for name in names_combined])
        
        if searched_name_in_combined or combined_name_in_searched:
            return True

        similarity_measure = NormalizedLevenshtein()
        names_similarities = [similarity_measure.similarity(name, search_name) for name in names_combined]

        try:
            if max(names_similarities) < NAME_SIMILARITY_THRESHOLD:
                return False
        except Exception as exc:
                logger.error(
                    "Conducting name quality check failed.",
                    exc=exc.__class__.__name__,
                    msg=str(exc),
                    search_name=search_name,
                    names_combined=names_combined,
                    names_similarities=names_similarities,
                )

        return True

    def _validate_properties(
        self,
        web_content: str,
        source: Dict[str, str],
        entity: Dict[str, Any],
        properties_list: Dict[str, str],
    ) -> Dict[str, str]:

        validated_properties_list = []
        proof_statements_list = []
        for properties in properties_list:
            if "date_of_birth" in properties and \
                isinstance(properties["date_of_birth"], str) and \
                    properties["date_of_birth"].lower() not in CHAT_UNKNOWN_STR_LOWER_VALUES:
                
                properties["date_of_birth"] = self._convert_date_value(
                    date_str=properties["date_of_birth"],
                    source_date=source["date"]
                )

            if "date_of_death" in properties and \
                isinstance(properties["date_of_death"], str) and \
                    properties["date_of_birth"].lower() not in CHAT_UNKNOWN_STR_LOWER_VALUES:

                properties["date_of_birth"] = self._convert_date_value(
                    date_str=properties["date_of_birth"],
                    source_date=source["date"]
                )

            validated_properties = {}
            proof_statements = {}
            for property_name, property_value in properties.items():
                try:
                    if property_value is None:
                        continue
                    if isinstance(property_value, str) and property_value.lower() in CHAT_UNKNOWN_STR_LOWER_VALUES:
                        continue

                    if not self._validate_property_value(property_name, property_value):
                        continue
                    
                    if property_name in ["first_name", "middle_name", "last_name"]:
                        if property_value.lower() not in web_content.lower():
                            continue

                    statement = f'{property_name.replace("_", " ").capitalize()} of {entity["properties"]["name"]} is {property_value}.'
                    
                    logger.info(
                        f"Verifying if {statement}",
                    )
                    
                    verdict = self.validotor.validate_statement(
                        statement,
                        web_content[:WEB_CONTENT_MAX_LEN],
                        self.prompt_validation_templates,
                        vote_type="majority"
                    )
                    
                    if verdict:
                        validated_properties[property_name] = property_value
                        proof_statements[property_name] = [
                            self.validotor.find_proof_of_statement(
                                statement,
                                web_content[:WEB_CONTENT_MAX_LEN],
                                proof_template
                            ) for proof_template in self.prompt_proof_templates
                        ]
                    else:
                        logger.info(
                            "Halucination detected",
                            property_name=property_name,
                            property_value=property_value
                        )
                except Exception as exc:
                    logger.error(
                            "Validating property failed",
                            exc=exc.__class__.__name__,
                            msg=str(exc),
                        )
                
            validated_properties_list.append(validated_properties)
            proof_statements_list.append(proof_statements)

        if len(validated_properties_list) == 0:
            return {}, {}
        if len(validated_properties_list) == 1:
            return merge_dicts(validated_properties_list[0]), merge_dicts(proof_statements_list[0])
        
        return merge_dicts(*validated_properties_list), merge_dicts(*proof_statements_list)

    def _validate_positions_held(
        self,
        web_content: str,
        source: Dict[str, str],
        entity: Dict[str, Any],
        positions_held_list: Dict[str, str],
    ) -> Dict[str, Dict[str, str]]:
        validated_positions = {}
        proof_statements = {}
        for positions_held in chain(positions_held_list):
            for position_name, position_time in positions_held.items():
            
                if isinstance(position_name, str) and position_name.lower() in CHAT_UNKNOWN_STR_LOWER_VALUES:
                    continue
                try:
                    start_time = position_time.get("start_time")
                    end_time = position_time.get("end_time")

                    if isinstance(start_time, str) and start_time.lower() in CHAT_UNKNOWN_STR_LOWER_VALUES:
                        start_time = None
                    if isinstance(end_time, str) and end_time.lower() in CHAT_UNKNOWN_STR_LOWER_VALUES:
                        end_time = None

                    statement = f'{entity["properties"]["name"]}'
                    if end_time:
                        statement += " was"
                    else:
                        statement += " is"

                    statement += f" {position_name}"

                    if start_time:
                        start_time = self._convert_date_value(start_time, source["date"])
                        position_time["start_time"] = end_time
                        if start_time:
                            statement += f" from {start_time}"
                    if end_time:
                        end_time = self._convert_date_value(end_time, source["date"])
                        position_time["end_time"] = end_time
                        if end_time:
                            statement += f" to {end_time}"

                    logger.info(
                        f"Verifying if {statement}",
                    )

                    verdict = self.validotor.validate_statement(
                        statement,
                        web_content[:WEB_CONTENT_MAX_LEN],
                        self.prompt_validation_templates,
                        vote_type="majority"
                    )

                    if verdict:
                        validated_positions[position_name] = position_time
                        proof_statements[position_name] = [
                            self.validotor.find_proof_of_statement(
                                statement,
                                web_content[:WEB_CONTENT_MAX_LEN],
                                proof_template
                            ) for proof_template in self.prompt_proof_templates
                        ]
                    else:
                        logger.info(
                            "Halucination detected",
                            position=position_name,
                        )

                except Exception as exc:
                    logger.error(
                            "Validating position failed",
                            exc=exc.__class__.__name__,
                            msg=str(exc),
                        )
        return validated_positions, proof_statements

    def _validate_associates(
        self,
        web_content: str,
        source: Dict[str, str],
        entity: Dict[str, Any],
        associates_list: Dict[str, List[str]],
    ) -> List[str]:
        associates_list = list(chain.from_iterable([associates.get("associates") for associates in associates_list]))
        if associates_list is None:
            return []
        if isinstance(associates_list, str) and associates_list.lower() in CHAT_UNKNOWN_STR_LOWER_VALUES:
            return []

        verified_associates = []
        proof_statements = []
        for associate in associates_list:
            if not validate_name(associate):
                continue
            try:
                statement = f'{associate} is an associate of {entity["properties"]["name"]}.'
                
                logger.info(
                    f"Verifying if {statement}",
                )
                
                verdict = self.validotor.validate_statement(
                    statement,
                    web_content[:WEB_CONTENT_MAX_LEN],
                    self.prompt_validation_templates,
                    vote_type="majority"
                )
                
                if verdict:
                    verified_associates.append(associate)
                    proof_statements.append([
                        self.validotor.find_proof_of_statement(
                            statement,
                            web_content[:WEB_CONTENT_MAX_LEN],
                            proof_template
                        ) for proof_template in self.prompt_proof_templates
                    ])
                else:
                    logger.info(
                        "Halucination detected",
                        associate=associate,
                    )

            except Exception as exc:
                logger.error(
                        "Validating associate failed",
                        exc=exc.__class__.__name__,
                        msg=str(exc),
                    )
        return verified_associates, proof_statements

    def _validate_relatives(
        self,
        web_content: str,
        source: Dict[str, str],
        entity: Dict[str, Any],
        relatives_list: Dict[str, List[str]],
    ) -> Dict[str, str]:
        validated_relatives = {}
        proof_statements = {}

        for relatives in chain(relatives_list):
            for relative_type, relatives in relatives.items():
                if isinstance(relatives, str):
                    relatives = [relatives]
                for relative in relatives:
                    if isinstance(relative, str) and relative.lower() in CHAT_UNKNOWN_STR_LOWER_VALUES:
                        continue
                    if not validate_name(relative):
                        continue
                    try:
                        statement = f'{relative} is a {relative_type} to {entity["properties"]["name"]}.'
                        
                        logger.info(
                            f"Verifying if {statement}",
                        )

                        verdict = self.validotor.validate_statement(
                            statement,
                            web_content[:WEB_CONTENT_MAX_LEN],
                            self.prompt_validation_templates,
                            vote_type="majority"
                        )

                        if verdict:
                            relative_proof_statements = [
                                self.validotor.find_proof_of_statement(
                                    statement,
                                    web_content[:WEB_CONTENT_MAX_LEN],
                                    proof_template
                                ) for proof_template in self.prompt_proof_templates
                            ]
                            try:
                                validated_relatives[relative_type].append(relative)
                                proof_statements[relative_type].append(relative_proof_statements)
                            except KeyError:
                                validated_relatives[relative_type] = [relative]
                                proof_statements[relative_type] = [relative_proof_statements]
                        else:
                            logger.info(
                                "Halucination detected",
                                relative_type=relative_type,
                                relative=relative
                            )
                    except Exception as exc:
                        logger.error(
                                "Validating relative failed",
                                exc=exc.__class__.__name__,
                                msg=str(exc),
                            )

        return validated_relatives, proof_statements

    def _validate_organizations(
        self,
        web_content: str,
        source: Dict[str, str],
        entity: Dict[str, Any],
        organizations_list: Dict[str, List[Dict[str, Any]]],
    ) -> List[Dict[str, str]]:
        validated_organizations = []
        proof_statements = []

        for organizations in organizations_list:
            for organization in organizations["organizations"]:
                try:
                    if isinstance(organization["organization_name"], str)\
                        and organization["organization_name"].lower() in CHAT_UNKNOWN_STR_LOWER_VALUES:

                        continue

                    statement = f'{entity["properties"]["name"]} is {organization["involvement_type"]} to {organization["organization_name"]}.'
                    logger.info(
                        f"Verifying if {statement}",
                    )

                    verdict = self.validotor.validate_statement(
                        statement,
                        web_content[:WEB_CONTENT_MAX_LEN],
                        self.prompt_validation_templates,
                        vote_type="majority"
                    )
                    if not verdict:
                        logger.info(
                            "Halucination detected",
                            organization=organization["organization_name"],
                            involvement=organization["involvement_type"],
                        )
                        continue
                    
                    organization_proof_statements = [
                        self.validotor.find_proof_of_statement(
                            statement,
                            web_content[:WEB_CONTENT_MAX_LEN],
                            proof_template
                        ) for proof_template in self.prompt_proof_templates
                    ]
                        
                    statement = f'{organization["organization_name"]} is {organization["organization_type"]}.'
                    logger.info(
                        f"Verifying if {statement}",
                    )

                    verdict = self.validotor.validate_statement(
                        statement,
                        web_content[:WEB_CONTENT_MAX_LEN],
                        self.prompt_validation_templates,
                        vote_type="majority"
                    )
                    if not verdict:
                        logger.info(
                            "Halucination detected",
                            organization=organization["organization_name"],
                            type=organization["organization_type"],
                        )
                        organization["organization_type"] = None
                    
                    date_start = organization.get("date_start")
                    date_end = organization.get("date_end")
                    if isinstance(date_start, str) and date_start.lower() in CHAT_UNKNOWN_STR_LOWER_VALUES:
                        date_start = None
                        organization["date_start"] = None
                    if date_start:
                        date_start = self._convert_date_value(date_start, source["date"])
                        organization["date_start"] = date_start
                            
                    if isinstance(date_end, str) and date_end.lower() in CHAT_UNKNOWN_STR_LOWER_VALUES:
                        date_end = None
                        organization["date_end"] = None
                    if date_end:
                        date_end = self._convert_date_value(date_end, source["date"])
                        organization["date_end"] = date_end
    
                    if date_start is None and date_end is None:
                        validated_organizations.append(organization)
                        proof_statements.append(organization_proof_statements)
                        continue

                    statement = f'{entity["properties"]["name"]} is {organization["involvement_type"]} to {organization["organization_name"]}'
                    if date_start is not None:
                        statement += f" from {date_start}"

                    if date_end is not None:
                        statement += f" to {date_end}"
                    statement += "."

                    logger.info(
                        f"Verifying if {statement}",
                    )

                    verdict = self.validotor.validate_statement(
                        statement,
                        web_content[:WEB_CONTENT_MAX_LEN],
                        self.prompt_validation_templates,
                        vote_type="majority"
                    )
                    if not verdict:
                        organization["date_start"] = None
                        organization["date_end"] = None
                    
                    validated_organizations.append(organization)
                    proof_statements.append(organization_proof_statements)

                except Exception as exc:
                    logger.error(
                            "Validating organization failed",
                            exc=exc.__class__.__name__,
                            msg=str(exc),
                        )

        return validated_organizations, proof_statements

    def _validate_addresses(
        self,
        web_content: str,
        source: Dict[str, str],
        entity: Dict[str, Any],
        addresses_list: Dict[str, List[Dict[str, Any]]],
    ) -> List[Dict[str, str]]:

        validated_addresses = []
        proof_statements = []
        
        for addresses in addresses_list:
            for address in addresses["addresses"]:
                try:
                    address = {
                        key: value if isinstance(value, str) and value.lower() not in CHAT_UNKNOWN_STR_LOWER_VALUES else "" 
                        for key, value in address.items()
                    }
                    
                    if address.get("street"):
                        if address.get("street").lower() not in web_content.lower():
                            continue
                            # street is known but not mentioned in web source, suspicious innit?

                    address["street_address"] = address.get("street", "") if address.get("street", "") is not None else ""
                    address["street_address"] += " " 
                    address["street_address"] += address.get("number", "") if address.get("number", "") is not None else ""
                    address["street_address"] = address["street_address"].strip()

                    try:
                        address = normalize_address(
                            address
                        )
                    except InvalidAddressError as e:
                        logger.warn(
                            "Invalid address detected",
                            **e.errors
                        )
                        continue

                    address_str = " ,".join(
                        [address_partial for address_partial in [
                                address["street_address"],
                                address["postal_code"],
                                address["city"],
                                address["city_area"],
                                address["country_area"],
                                address["city"],
                                address["country_code"],
                            ] if isinstance(address_partial, str) and len(address_partial) > 0
                        ]
                    )
                    statement = f'{entity["properties"]["name"]} {address["address_type"]} is {address_str}.'
                    logger.info(
                        f"Verifying if {statement}",
                    )

                    verdict = self.validotor.validate_statement(
                        statement,
                        web_content[:WEB_CONTENT_MAX_LEN],
                        self.prompt_validation_templates,
                        vote_type="majority"
                    )
                    if not verdict:
                        logger.info(
                            "Halucination detected",
                            address=address_str,
                            type=address["address_type"],
                        )
                    
                    if verdict:
                        validated_addresses.append(address)
                        proof_statements.append([
                            self.validotor.find_proof_of_statement(
                                statement,
                                web_content[:WEB_CONTENT_MAX_LEN],
                                proof_template
                            ) for proof_template in self.prompt_proof_templates
                        ])
                        
                except Exception as exc:
                    logger.error(
                            "Validating address failed",
                            exc=exc.__class__.__name__,
                            msg=str(exc),
                            address=address,
                        )
        return validated_addresses, proof_statements

    def _postprocess_properties(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        entity = self._postprocess_names(entity)
        entity = self._postprocess_titles(entity)
        entity = self._postprocess_countries(entity)
        entity = self._postprocess_dates(entity)
        entity = self._postprocess_ids(entity)

        return entity

    def _postprocess_names(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        return entity

    def _postprocess_titles(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        return entity

    def _postprocess_countries(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        return entity

    def _postprocess_dates(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        return entity

    def _postprocess_ids(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        return entity

    def _postprocess_positions_held(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        return entity

    def _postprocess_associates(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        return entity

    def _postprocess_relatives(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        return entity

    def _postprocess_organizations(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        return entity

    def _postprocess_addresses(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        return entity

    def _merge_web_source_properties(self, web_source_properties: List[Dict[str, Any]]) -> Dict[str, Any]:
        return merge_dicts(*web_source_properties, skipkeys="source")

    def _validate_property_value(self, property_name: str, property_value: str) -> bool:
        match property_name:
            case "gender":
                pass
            case"title":
                pass
            case "first_name":
                pass
            case "middle_name":
                pass
            case "last_name":
                pass
            case  "alias":
                pass
            case "date_of_birth":
                pass
            case "date_of_death":
                pass
            case "residence_country":
                pass
            case "nalionality":
                pass
            case "id_numbers":
                pass
            case "address":
                pass

        return True

    def _convert_date_value(self, date_str, source_date=None) -> str:
        try:
            converted_datetime_str = pd.to_datetime(date_str).strftime("%Y-%m-%d")
        except DateParseError:
            logger.info(
                "Converting datetime str to %Y-%m-%d",
                date_str=date_str
            )
            
            input_str = "Convert datetime string to %Y-%m_-%d format."
            if source_date:
                input_str += " Current date is {}."
            input_str += f" Datetime string: {date_str}. Return direct answer. Return no answer if datetime string has invalid format."

            converted_datetime_str = ask_chat(
                cached_response_client=self.llm_client,
                input_str=input_str,
            )
            try:
                converted_datetime_str = pd.to_datetime(converted_datetime_str).strftime("%Y-%m-%d")
            except DateParseError:
                converted_datetime_str = None

        return converted_datetime_str

    @staticmethod
    def _get_entity_web_search_strings(entity: Dict[str, Any]):

        name = entity["properties"]["name"]
        country = entity["properties"]["country"]
        search_phrases = []
        
        if "position" in entity["properties"]:
            search_phrases.append(f'{name} {entity["properties"]["position"]} in {country}')

        if "searched_position" in entity:
            search_phrases.append(f'{name} {entity["searched_position"]} in {country}')
            
        if len(search_phrases) == 0:
            search_phrases.append(f"{name}, {country}")
        
        return search_phrases
