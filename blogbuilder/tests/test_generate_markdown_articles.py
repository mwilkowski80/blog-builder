import pytest

from blogbuilder.generate_markdown_articles import _remove_first_line, _remove_last_line


@pytest.mark.parametrize("input_text,expected_text", [
    ("70\nsome text", "some text"),
    ("20", ""),
    ("", ""),
    ("\n\n", ""),
    (None, None),
    ("30\r\nhello world", "hello world"),
    ("35\r\n\r\nhello world", "\r\nhello world"),
    ("\n33\n\nhello world", "\nhello world"),
])
def test_remove_first_line(input_text, expected_text):
    assert _remove_first_line(input_text) == expected_text


@pytest.mark.parametrize("input_text,expected_text", [
    ("70\nsome text", "70"),
    ("20", ""),
    ("", ""),
    ("\n\n", ""),
    (None, None),
    ("30\r\nhello world", "30"),
    ("35\r\n\r\nhello world", "35"),
    ("\n33\n\nhello world", "33"),
])
def test_remove_last_line(input_text, expected_text):
    assert _remove_last_line(input_text) == expected_text
