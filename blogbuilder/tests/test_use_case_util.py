import pytest as pytest

from blogbuilder.generate_raw_articles_use_case import _extract_int_from_llm_output


def test_extract_int_from_llm_output_exception_if_empty():
    with pytest.raises(ValueError):
        _extract_int_from_llm_output(None)

    with pytest.raises(ValueError):
        _extract_int_from_llm_output(' ')


def test_extract_int_from_llm_output_extract_number_from_the_first_line():
    assert _extract_int_from_llm_output("""
    70\nsome text
    """) == 70

    assert _extract_int_from_llm_output('20') == 20
    assert _extract_int_from_llm_output('30\r\nhello world') == 30
    assert _extract_int_from_llm_output('35\r\n\r\nhello world') == 35
    assert _extract_int_from_llm_output('\n33\n\nhello world') == 33
    assert _extract_int_from_llm_output('\n100. The page is about this\nand about something else') == 100
