import pytest

from databricks.labs.blueprint.tui import MockPrompts, Prompts


def test_choices_out_of_range(mocker):
    prompts = Prompts()
    mocker.patch("builtins.input", return_value="42")
    with pytest.raises(ValueError):
        prompts.choice("foo", ["a", "b"])


def test_choices_not_a_number(mocker):
    prompts = Prompts()
    mocker.patch("builtins.input", return_value="two")
    with pytest.raises(ValueError):
        prompts.choice("foo", ["a", "b"])


def test_choices_happy(mocker):
    prompts = Prompts()
    mocker.patch("builtins.input", return_value="1")
    res = prompts.choice("foo", ["a", "b"])
    assert "b" == res


def test_ask_for_int():
    prompts = MockPrompts({r".*": ""})
    res = prompts.question("Number of threads", default="8", valid_number=True)
    assert "8" == res


def test_extend_prompts():
    prompts = MockPrompts({r"initial_question": "initial_answer"})

    # Test that the initial question is mocked
    res = prompts.question("initial_question")
    assert "initial_answer" == res

    # Test that the new question is not mocked
    with pytest.raises(ValueError, match="not mocked: new_question"):
        prompts.question("new_question")

    # Test that the new question is mocked after using extend
    new_prompts = prompts.extend({r"new_question": "new_answer"})
    res = new_prompts.question("new_question")
    assert "new_answer" == res

    # Test that new question is still not mocked in the original prompts
    with pytest.raises(ValueError, match="not mocked: new_question"):
        prompts.question("new_question")


def test_choice_uses_pagination(mocker):
    """Test that choice() uses pagination when list > 10 items"""
    prompts = Prompts()
    # Test that pagination is show and input n will change to page 2 and select option 11
    mocker.patch("builtins.input", side_effect=["n", "12"])

    choices = [
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i",
        "j",
        "k",
        "l",
        "m",
    ]
    res = prompts.choice("foo", choices)
    assert "m" == res
