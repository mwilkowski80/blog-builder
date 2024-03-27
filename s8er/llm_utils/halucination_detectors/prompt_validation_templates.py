from abc import ABC, abstractmethod


class PromptValidationTemplate(ABC):
    """
    Abstract class for chat output validation templates.
    """
    @abstractmethod
    def __call__(self, statement: str, text: str) -> str:
        """
        Ouput string shoud be a question for Chat model.
        E.g.
        Based on text model says that "Never gonna give you up" is Rick Ashely's song. Our statement might be:
        '"Never gonna give you up" is Rick Ashely's song.'

        And template output might be:
        "
        Based on text: <text>, answer if sentence is correct: '"Never gonna give you up" is Rick Ashely's song.'.
        "

        Args:
            statement (str): Statement based on fact.
            text (str): Source text fact was extracted from.

        Returns:
            str: Question to ask regarding facts from text.
        """
        pass
    

class IsSentenceSuportedByText(PromptValidationTemplate):
    def __call__(self, statement: str, text: str) -> str:
        return f"""
Is the statement supported by the text? 

Statement: <{statement}>
Text: <{text}>
Answer yes or no without explanation.
Do not focus on your knowledge, focus on text only.
""".strip()


class IsSentenceSuportedByText2(PromptValidationTemplate):
    def __call__(self, statement: str, text: str) -> str:
        return f"""
Statement: <{statement}>
Text: <{text}>
Is the statement supported by the text above? 
Answer yes or no without explanation.
Do not focus on your knowledge, focus on text only.
""".strip()


class IsSentenceSuportedByContext(PromptValidationTemplate):
    def __call__(self, statement: str, text: str) -> str:
        return f"""
Is the statement supported by the context? 
Context: <{statement}>
Text: <{text}>
Answer yes or no without explanation.
Do not focus on your knowledge, focus on text only.
""".strip()


class IsSentenceSuportedByContext2(PromptValidationTemplate):
    def __call__(self, statement: str, text: str) -> str:
        return f"""
Context: <{statement}>
Text: <{text}>
Is the statement supported by the context above? 
Answer yes or no without explanation.
Do not focus on your knowledge, focus on text only.
""".strip()


class ProveStatementWithText(PromptValidationTemplate):
    def __call__(self, statement: str, text: str) -> str:
        return f"""
Statement: <{statement}>
Text: <{text}>
Based on text source, return sentence supporting the statement.
""".strip()


class ProveStatementWithText2(PromptValidationTemplate):
    def __call__(self, statement: str, text: str) -> str:
        return f"""
Text: <{text}>
Based on text extract the direct sentence suppotrting the statement: "{statement}".
""".strip()


class ProveStatementWithText3(PromptValidationTemplate):
    def __call__(self, statement: str, text: str) -> str:
        return f"""
Text: <{text}>
Based on text extract the direct sentence suppotrting the statement: "{statement}".

If there is no such sentence, return "No supporting sentence" answer.
""".strip()
