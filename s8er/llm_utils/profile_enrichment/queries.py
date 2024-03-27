from typing import Dict


def _ask_for_entity_properties_input_str_0(entity: Dict, web_content: str) -> str:
    return f"""based on text: <{web_content[:12000]}>
extract information regarding {entity["properties"]["name"]}

Include information such as: ["gender", "title", "first_name", "middle_name", "last_name", "alias", "date_of_birth", "date_of_death", "residence_country", "nalionality", "id_numbers"]
Answer in the following json format:
{{"<property_name>": <property_values>}}

If value is unknown use null value.
"""

def _ask_for_entity_positions_held_input_str_0(entity: Dict, web_content: str) -> str:
    return f"""based on text: <{web_content[:12000]}>
extract information regarding positions held by {entity["properties"]["name"]}

Answer in the following json format:
{{"<position_held>": {{"start_date": "<start_date_value>", "end_date": "<end_date_vlues>"}}}}

If value is unknown use null value. Use "%Y-%m-%d" datetime string format.
"""

def _ask_for_entity_associates_input_str_0(entity: Dict, web_content: str) -> str:
    return f"""based on text: <{web_content[:12000]}>
extract information regarding associates of {entity["properties"]["name"]}

Answer in the following json format:
{{"associates": <list of associates names>}}
"""

def _ask_for_entity_organizations_input_str_0(entity: Dict, web_content: str) -> str:
    return f"""
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

def _ask_for_entity_relatives_input_str_0(entity: Dict, web_content: str) -> str:
    return f"""based on text: <{web_content[:12000]}>
extract information regarding family relatives of {entity["properties"]["name"]}

Answer in the following json format:
{{"<relationship_name>": <list of related persons>}}
Anser only when full name of the related person is known.
"""

def _ask_for_entity_addresses_input_str_0(entity: Dict, web_content: str) -> str:
    return f"""based on text: <{web_content[:12000]}>
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
"""


ASK_FOR_PROPERTIES_INPUT_STRINGS = [
    _ask_for_entity_properties_input_str_0,
]
ASK_FOR_POSITIONS_HELD_INPUT_STRINGS = [
    _ask_for_entity_positions_held_input_str_0,
]
ASK_FOR_ASSOCIATES_INPUT_STRINGS = [
    _ask_for_entity_associates_input_str_0,
]
ASK_FOR_ORGANIZATIONS_INPUT_STRINGS = [
    _ask_for_entity_organizations_input_str_0,
]
ASK_FOR_RELATIVES_INPUT_STRINGS = [
    _ask_for_entity_relatives_input_str_0,
]
ASK_FOR_ADDRESSES_INPUT_STRINGS = [
    _ask_for_entity_addresses_input_str_0,
]