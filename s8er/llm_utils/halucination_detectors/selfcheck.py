from abc import ABC, abstractmethod
from typing import Any, Callable, List

from s8er.llm_utils.commons import ask_chat, answer_to_bool
from s8er.llm_utils.halucination_detectors.prompt_validation_templates import PromptValidationTemplate


class AbstractValidator(ABC):
    """
    Abstract class for any llm model output validator.
    """
    def __init__(self, llm_client: Callable, web_search_api_client: Callable) -> None:
        """_summary_

        Args:
            llm_client (Callable): LLM model to be used for validation
            web_search_api_client (Callable): Web Search API to be used in validation
        """
        super().__init__()

        self.llm_client = llm_client
        self.web_search_api_client = web_search_api_client

    @abstractmethod
    def validate_by_multiple_statements(
        self,
        statements: List[str],
        text: str,
        prompt_templates: List[PromptValidationTemplate],
        statements_vote_type: str = "halved",
        templates_vote_type: str = "majority",
    ) -> bool:
        """
        Validate multiple statements to find if chat answer is covered by text.

        Args:
            statements (List[str]): List of statemnts regarding chat output
            text (str): Context for chat's generated output
            prompt_templates (List[PromptValidationTemplate]): list of templates for statement verification
            statements_vote_type (str, optional): Defaults to "halved".
            templates_vote_type (str, optional): Defaults to "majority".

        Returns:
            bool: Voted decision if output is factual.
        """
        votes = [
            self.validate_statement(
                statement,
                text,
                prompt_templates,
                vote_type=templates_vote_type
            ) for statement in statements
        ]
        return self.vote_response(statements_vote_type, votes)

    @abstractmethod
    def validate_statement(
        self,
        statement: str,
        text: str,
        prompt_templates: List[PromptValidationTemplate],
        vote_type: str = "majority",
    ) -> bool:
        """
        Validate statement to find if chat answer is covered by text.

        Args:
            statement (str): Statement with chat output
            text (str): Context for chat's generated output
            prompt_templates (List[PromptValidationTemplate]): list of templates for statement verification
            vote_type (str, optional): Defaults to "majority".

        Returns:
            bool: Voted decision if output is factual.
        """
        self_validation = [False for _ in prompt_templates]

        for i, template in enumerate(prompt_templates):
            self_validation[i] = self.validate_statement_with_template(
                statement, text, template
            )

        return self.vote_response(vote_type, self_validation)

    @abstractmethod
    def validate_statement_with_template(
        self,
        statement: str,
        text: str,
        prompt_template: PromptValidationTemplate,
    ) -> bool:
        """
        Validate statement with single question template.

        Args:
            statement (str): Statement with chat output
            text (str): Context for chat's generated output
            prompt_template (PromptValidationTemplate): Chat output verification template

        Returns:
            bool: Model response
        """
        return answer_to_bool(
            ask_chat(
                self.llm_client,
                prompt_template(
                    statement, text
                )
            ),
            default=False
        )

    @abstractmethod
    def find_proof_of_statement(
        self,
        statement: str,
        text,
        prompt_template: PromptValidationTemplate,
    ) -> str:
        return ask_chat(
            self.llm_client,
            prompt_template(
                statement, text
            )
        )

    @staticmethod
    def vote_response(
        vote_type: str,
        votes: List[bool],
    ) -> bool:
        """

        Args:
            vote_type (str): Vote type, one of:
                - all: all responses are True
                - any: any response was True
                - halved: at least half of responses was True
                - majority: over half of responses was True
            votes (List[bool]): List of model resonses

        Raises:
            NotImplementedError: Unknown vote type was passed

        Returns:
            bool: Voted decision
        """
        match vote_type:
            case "all":
                return all(votes)
            case "any":
                return any(votes)
            case "majority":
                return sum(votes) / len(votes) > 0.5
            case "halved":
                return sum(votes) / len(votes) >= 0.5
            case _:
                raise NotImplementedError("vote type %s not implemented. Use all, any, majority or halved".format(vote_type))


class BaseSelfValidator(AbstractValidator):
    def __init__(self, llm_client: Callable[..., Any], web_search_api_client: Callable[..., Any]) -> None:
        super().__init__(llm_client, web_search_api_client)

    def validate_by_multiple_statements(self, statements: List[str], text: str, prompt_templates: List[PromptValidationTemplate], statements_vote_type: str = "halved", templates_vote_type: str = "majority") -> bool:
        return super().validate_by_multiple_statements(statements, text, prompt_templates, statements_vote_type, templates_vote_type)
    
    def validate_statement(self, statement: str, text: str, prompt_templates: List[PromptValidationTemplate], vote_type: str = "majority") -> bool:
        return super().validate_statement(statement, text, prompt_templates, vote_type)
    
    def validate_statement_with_template(self, statement: str, text: str, prompt_template: PromptValidationTemplate) -> bool:
        return super().validate_statement_with_template(statement, text, prompt_template)

    def find_proof_of_statement(self, statement: str, text, prompt_template: PromptValidationTemplate) -> str:
        return super().find_proof_of_statement(statement, text, prompt_template)
