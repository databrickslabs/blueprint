"""Text User Interface (TUI) utilities"""

from __future__ import annotations

import getpass
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from typing import TypeVar

logger = logging.getLogger(__name__)


T = TypeVar("T")


class Prompts:
    """`input()` builtin on steroids"""

    _REGEX_NUMBER = re.compile(r"^\d+$")
    _REGEX_YES_NO = re.compile(r"[Yy][Ee][Ss]|[Nn][Oo]")

    def multiple_choice_from_dict(self, item_prompt: str, choices: Mapping[str, T]) -> Sequence[T]:
        """Use to select multiple items from a mapping.

        :param item_prompt: str:
        :param choices: Mapping[str, T]:

        """
        selected: list[T] = []
        dropdown = {"[DONE]": "done", **choices}
        while True:
            key = self.choice(item_prompt, list(dropdown.keys()))
            if key == "[DONE]":
                break
            selected.append(choices[key])
            del dropdown[key]
            if len(selected) == len(choices):
                # we've selected everything
                break
        return selected

    def choice_from_dict(self, text: str, choices: Mapping[str, T], *, sort: bool = True) -> T:
        """Use to select a value from the dictionary by showing users sorted dictionary keys

        :param text: str:
        :param choices: Mapping[str,T]:
        :param *:
        :param sort: bool:  (Default value = True)

        """
        key = self.choice(text, list(choices.keys()), sort=sort)
        return choices[key]

    def choice(self, text: str, choices: Sequence[str], *, max_attempts: int = 10, sort: bool = True) -> str:
        """Use to select a value from a list

        :param text: str:
        :param choices: Sequence[str]:
        :param *:
        :param max_attempts: int:  (Default value = 10)
        :param sort: bool:  (Default value = True)

        """
        if sort:
            # This forces our choices argument to be a sequence of strings.
            choices = sorted(choices, key=str.casefold)
        numbered = "\n".join(f"\033[1m[{i}]\033[0m \033[36m{v}\033[0m" for i, v in enumerate(choices))
        prompt = f"\033[1m{text}\033[0m\n{numbered}\nEnter a number between 0 and {len(choices) - 1}"
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            res = int(self.question(prompt, valid_number=True))
            if res >= len(choices) or res < 0:
                print(f"\033[31m[ERROR] Out of range: {res}\033[0m\n")
                continue
            return choices[res]
        msg = f"cannot get answer within {max_attempts} attempt"
        raise ValueError(msg)

    def confirm(self, text: str, *, max_attempts: int = 10) -> bool:
        """Use to guard any optional or destructive actions of your app

        :param text: str:
        :param *:
        :param max_attempts: int:  (Default value = 10)

        """
        answer = self.question(text, valid_regex=self._REGEX_YES_NO, default="no", max_attempts=max_attempts)
        return answer.lower() == "yes"

    def question(
        self,
        text: str,
        *,
        default: str | None = None,
        max_attempts: int = 10,
        valid_number: bool = False,
        valid_regex: str | re.Pattern | None = None,
        validate: Callable[[str], bool] | None = None,
    ) -> str:
        """Use as testable alternative to `input()` builtin

        :param text: str:
        :param *:
        :param default: str | None:  (Default value = None)
        :param max_attempts: int:  (Default value = 10)
        :param valid_number: bool:  (Default value = False)
        :param valid_regex: str | None:  (Default value = None)
        :param validate: Callable[[str], bool] | None:  (Default value = None)

        """
        default_help = "" if default is None else f"\033[36m (default: {default})\033[0m"
        prompt = f"\033[1m{text}{default_help}: \033[0m"
        match_regex: re.Pattern | None
        if valid_number:
            match_regex = self._REGEX_NUMBER
        elif isinstance(valid_regex, str):
            match_regex = re.compile(valid_regex)
        else:
            match_regex = valid_regex
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            res = input(prompt)  # pylint: disable=bad-builtin
            if res and validate:
                if not validate(res):
                    continue
            if res and match_regex:
                if not match_regex.match(res):
                    print(f"\033[31m[ERROR] Not a '{match_regex.pattern}' match: {res}\033[0m\n")
                    continue
                return res
            if not res and default:
                return default
            if not res:
                continue
            return res
        raise ValueError(f"cannot get answer within {max_attempts} attempt")

    def password(self, text: str, *, max_attempts: int = 10) -> str:
        """
        Secure input for passwords (hidden input).
        :param text: str: Prompt message
        :param max_attempts: int: Max attempts before failing
        """
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            passwd = getpass.getpass(f"\033[1m{text}\033[0m:")
            if passwd:
                return passwd
        raise ValueError(f"cannot get password within {max_attempts} attempts")


class MockPrompts(Prompts):
    """Testing utility for prompts"""

    def __init__(self, patterns_to_answers: Mapping[str, str]):
        patterns = [(re.compile(k), v) for k, v in patterns_to_answers.items()]
        self._questions_to_answers = sorted(patterns, key=lambda _: len(_[0].pattern), reverse=True)

    def question(self, text: str, default: str | None = None, **_) -> str:
        logger.info(f"Asking prompt: {text}")
        for question, answer in self._questions_to_answers:
            if not question.search(text):
                continue
            if not answer and default:
                return default
            return answer
        raise ValueError(f"not mocked: {text}")

    def extend(self, patterns_to_answers: Mapping[str, str]) -> MockPrompts:
        """Extend the existing list of questions and answers"""
        new_patterns_to_answers = {
            **{pattern.pattern: answer for pattern, answer in self._questions_to_answers},
            **patterns_to_answers,
        }
        return MockPrompts(new_patterns_to_answers)

    def password(self, text: str, **_) -> str:
        logger.info(f"Mock password prompt: {text}")
        return self.question(text, **_)
