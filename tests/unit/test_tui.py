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
    res = prompts.question("initial_question")
    assert "initial_answer" == res

    with pytest.raises(ValueError) as err:
        prompts.question("new_question")
    assert "not mocked: new_question" == err.value.args[0]

    prompts.extend({r"new_question": "new_answer"})
    res = prompts.question("new_question")
    assert "new_answer" == res


