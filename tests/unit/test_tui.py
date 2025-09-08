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


def test_password_happy(mocker):
    prompts = Prompts()
    mocker.patch("getpass.getpass", return_value="secret")
    res = prompts.password("Enter password")
    assert res == "secret"


def test_password_max_attempts(mocker):
    prompts = Prompts()
    mocker.patch("getpass.getpass", return_value="")
    with pytest.raises(ValueError, match="cannot get password within 10 attempts"):
        prompts.password("Enter password")
