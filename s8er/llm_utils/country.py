from typing import Callable, Dict, Any, List
import os
from structlog.stdlib import get_logger as get_raw_logger
from itertools import chain
import json

from s8er.llm_utils.commons import get_ddg_search
from s8er.llm_utils.position_processors.position_finder import (
    AbstarctPositionFinder,
    StandardPositionFinder,
    StateGovenmentPositionFinder,
    CitiesPositionFinder,
    ExecutiveBranchOfGovenmentPositionFinder,
    JudicialBranchOfGovenmentPositionFinder,
    LegislativeBranchOfGovenmentPositionFinder,
)
from s8er.llm_utils.position_processors.position_extractor import (
    AbstractPositionExtractor,
    StandardPositionExtractor,
    StateGovenmentPositionExtractor,
    CitiesPositionExtractor,
)
from s8er.llm_utils.position_processors.position_validator import (
    AbstarctPositionValidator,
    StandardPositionValidator,
    StateGovernmentPositionValidator,
)
from s8er.llm_utils.profile_enrichment.person_profile_enrichment import PersonProfileEnricher


logger = get_raw_logger(os.path.basename(__file__))


class PepCountryProcessor:
    """
    E2E country to list of enriched entities processor by specified country.
    """
    def __init__(self, llm_client: Callable, config: dict) -> None:
        self.llm_client = llm_client
        self.config = config

    def get_entities(self, country: str, debug_save_path: str = None) -> List[Dict[str, Any]]:
        """
        Process one country and get list of entities for it.

        Args:
            country (str): Country of interest

        Returns:
            List[Dict[str, Any]]: List of web enriched entities of specified country.
        """
        entities = {}
        for processor_name, processor in self.config["processors"].items():

            logger.info(f"Running {processor_name} processor...")

            finder, extractor, validator = self._init_processor(processor)

            logger.info(f"Running {processor_name} finder...")
            positions_found = finder.find_positions(
                country=country,
                **processor.get("finder").get("find_positions_kwargs"),
            )
            logger.info(f"Number of positions found: {len(positions_found)}")
            
            if debug_save_path:
                with open(f"{debug_save_path}/{country.lower().replace(' ', '_')}_positions_found_{processor_name}.json", "w") as f:
                    json.dump(positions_found, f, indent=4)

            logger.info(f"Running {processor_name} extractor...")
            extracted_entities = extractor.get_entities(
                country=country,
                position_finder_results=positions_found,
                **processor.get("extractor").get("get_entities_kwargs"),
            )
            logger.info(f"Number of extracted persons: {len(extracted_entities)}")
            
            if debug_save_path:
                with open(f"{debug_save_path}/{country.lower().replace(' ', '_')}_extracted_entities_{processor_name}.json", "w") as f:
                    json.dump(extracted_entities, f, indent=4)


            logger.info(f"Running {processor_name} validator...")
            entities[processor_name] = validator.validate_entities(
                extracted_entities=extracted_entities,
                **processor.get("validator").get("validate_entities_kwargs"),
            )
            logger.info(f"Number of extracted persons after validation: {len(entities[processor_name])}")
            
            if debug_save_path:
                with open(f"{debug_save_path}/{country.lower().replace(' ', '_')}_validated_entities_{processor_name}.json", "w") as f:
                    json.dump(entities[processor_name], f, indent=4)

        entities = list(chain.from_iterable(entities.values()))
        logger.info(f"Number of entities from all processors: {len(entities)}")

        for enricher_name, enrichment_processor in self.config["enrichers"].items():
            logger.info(f"Running {enricher_name} processor...")

            profile_enrichment_processor = self._get_processor(enrichment_processor.get("name"))(
                llm_client=self.llm_client,
                web_search_api_client=get_ddg_search(**enrichment_processor.get("web_search_api_kwargs")),
            )
            enriched_entities = profile_enrichment_processor.enrich_entities(
                entities,
                # savepath="/Users/michal.jarzyna/repos/experiment-pep-wikidata-analysis/data/enrich-test-burkina_faso.jsonl",
                savepath=f"{'/'.join(debug_save_path.split('/')[:-1])}/enrich-test-{country.lower().replace(' ', '_')}.jsonl"
            )

        logger.info("Porcess finished.")
        return enriched_entities

    def _get_processor(self, processor: str):
        match processor:
            case "StandardPositionFinder":
                return StandardPositionFinder
            case "StandardPositionExtractor":
                return StandardPositionExtractor
            case "StandardPositionValidator":
                return StandardPositionValidator

            case "StateGovenmentPositionFinder":
                return StateGovenmentPositionFinder
            case "StateGovenmentPositionExtractor":
                return StateGovenmentPositionExtractor
            case "StateGovernmentPositionValidator":
                return StateGovernmentPositionValidator

            case "CitiesPositionFinder":
                return CitiesPositionFinder
            case "CitiesPositionExtractor":
                return CitiesPositionExtractor

            case "ExecutiveBranchOfGovenmentPositionFinder":
                return ExecutiveBranchOfGovenmentPositionFinder

            case "JudicialBranchOfGovenmentPositionFinder":
                return JudicialBranchOfGovenmentPositionFinder

            case "LegislativeBranchOfGovenmentPositionFinder":
                return LegislativeBranchOfGovenmentPositionFinder

            case "PersonProfileEnricher":
                return PersonProfileEnricher

            case _:
                raise NameError(f"Porcessor {processor} not found.")
    
    def _init_processor(self, processor_config: Dict[str, Any]) -> (
        AbstarctPositionFinder,
        AbstractPositionExtractor,
        AbstarctPositionValidator,
    ):
        finder_name = processor_config.get("finder").get("name")
        finder = self._get_processor(finder_name)(
            llm_client=self.llm_client,
            web_search_api_client=get_ddg_search(**processor_config.get("finder").get("web_search_api_kwargs", {}))
        )

        extractor_name = processor_config.get("extractor").get("name")
        extractor = self._get_processor(extractor_name)(
            llm_client=self.llm_client,
            web_search_api_client=get_ddg_search(**processor_config.get("extractor").get("web_search_api_kwargs", {}))
        )

        validator_name = processor_config.get("validator").get("name")
        validator = self._get_processor(validator_name)(
            llm_client=self.llm_client,
            web_search_api_client=get_ddg_search(**processor_config.get("validator").get("web_search_api_kwargs", {}))
        )

        return finder, extractor, validator
