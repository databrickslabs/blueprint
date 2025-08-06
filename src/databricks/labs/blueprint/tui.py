"""Text User Interface (TUI) utilities"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class Prompts:
    """`input()` builtin on steroids"""

    def multiple_choice_from_dict(self, item_prompt: str, choices: dict[str, Any]) -> list[Any]:
        """Use to select multiple items from dictionary

        :param item_prompt: str:
        :param choices: dict[str, Any]:

        """
        selected: list[Any] = []
        dropdown = {"[DONE]": "done"} | choices
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

    def choice_from_dict(self, text: str, choices: dict[str, Any], *, sort: bool = True) -> Any:
        """Use to select a value from the dictionary by showing users sorted dictionary keys

        :param text: str:
        :param choices: dict[str,Any]:
        :param *:
        :param sort: bool:  (Default value = True)

        """
        key = self.choice(text, list(choices.keys()), sort=sort)
        return choices[key]

    @staticmethod
    def _clear_screen():
        """Clear terminal screen cross-platform"""
        os.system("cls" if os.name == "nt" else "clear")

    def _display_page_and_get_input(
        self, text: str, page_choices: list[Any], current_page: int, total_pages: int, start: int, end: int
    ) -> str:
        """Display current page and get user input"""
        display = [f"\033[1m[{start + i}]\033[0m \033[36m{choice}\033[0m" for i, choice in enumerate(page_choices)]
        numbered = "\n".join(display)

        nav_info = []
        if current_page > 0:
            nav_info.append("\033[1m\033[36m'p' for previous\033[0m")
        if current_page < total_pages - 1:
            nav_info.append("\033[1m\033[36m'n' for next\033[0m")
        nav_text = f" ({', '.join(nav_info)})" if nav_info else ""

        page_info = f"\033[1m\033[36mPage {current_page + 1} of {total_pages}\033[0m"
        prompt = (
            f"\033[1m{text}\033[0m ({page_info}){nav_text}\n{numbered}\nEnter a number between {start} and {end - 1}"
        )

        return self.question(prompt, valid_number=False)

    def _paginate_choices(self, text: str, choices: list[Any], *, max_attempts: int = 10, page_size: int = 10) -> str:
        """Handle paginated choice selection for large lists"""
        total_pages = (len(choices) + page_size - 1) // page_size
        current_page = 0
        attempt = 0

        while attempt < max_attempts:
            start = current_page * page_size
            end = min(start + page_size, len(choices))
            page_choices = choices[start:end]

            user_input = self._display_page_and_get_input(text, page_choices, current_page, total_pages, start, end)

            if user_input.lower() == "p" and current_page > 0:
                current_page -= 1
                self._clear_screen()
                continue

            if user_input.lower() == "n" and current_page < total_pages - 1:
                current_page += 1
                self._clear_screen()
                continue

            try:
                res = int(user_input)
                if start <= res < end:
                    return choices[res]
                print(f"\033[31m[ERROR] Out of range: {res}\033[0m\n")
            except ValueError:
                print(f"\033[31m[ERROR] Invalid input: {user_input}\033[0m\n")

            attempt += 1

        raise ValueError(f"Max attempts ({max_attempts}) exceeded")

    def choice(
        self, text: str, choices: list[Any], *, max_attempts: int = 10, sort: bool = True, page_size: int = 10
    ) -> str:
        """Use to select a value from a list with automatic pagination for large lists

        :param text: str:
        :param choices: list[Any]:
        :param *:
        :param max_attempts: int:  (Default value = 10)
        :param sort: bool:  (Default value = True)
        :param page_size: int:  (Default value = 10)
        """
        if sort:
            choices = sorted(choices, key=str.casefold)

        if len(choices) > page_size:
            return self._paginate_choices(text, choices, max_attempts=max_attempts, page_size=page_size)

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

    def confirm(self, text: str, *, max_attempts: int = 10):
        """Use to guard any optional or destructive actions of your app

        :param text: str:
        :param *:
        :param max_attempts: int:  (Default value = 10)

        """
        answer = self.question(text, valid_regex=r"[Yy][Ee][Ss]|[Nn][Oo]", default="no", max_attempts=max_attempts)
        return answer.lower() == "yes"

    def question(
        self,
        text: str,
        *,
        default: str | None = None,
        max_attempts: int = 10,
        valid_number: bool = False,
        valid_regex: str | None = None,
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
        match_regex = None
        if valid_number:
            valid_regex = r"^\d+$"
        if valid_regex:
            match_regex = re.compile(valid_regex)
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            res = input(prompt)  # pylint: disable=bad-builtin
            if res and validate:
                if not validate(res):
                    continue
            if res and match_regex:
                if not match_regex.match(res):
                    print(f"\033[31m[ERROR] Not a '{valid_regex}' match: {res}\033[0m\n")
                    continue
                return res
            if not res and default:
                return default
            if not res:
                continue
            return res
        raise ValueError(f"cannot get answer within {max_attempts} attempt")


class MockPrompts(Prompts):
    """Testing utility for prompts"""

    def __init__(self, patterns_to_answers: dict[str, str]):
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

    def extend(self, patterns_to_answers: dict[str, str]) -> MockPrompts:
        """Extend the existing list of questions and answers"""
        new_patterns_to_answers = {
            **{pattern.pattern: answer for pattern, answer in self._questions_to_answers},
            **patterns_to_answers,
        }
        return MockPrompts(new_patterns_to_answers)
