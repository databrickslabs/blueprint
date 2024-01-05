Databricks Labs Blueprint
---

[![python](https://img.shields.io/badge/python-3.10,%203.11,%203.12-green)](https://github.com/databrickslabs/blueprint/actions/workflows/push.yml)
[![codecov](https://codecov.io/github/databrickslabs/blueprint/graph/badge.svg?token=x1JSVddfZa)](https://codecov.io/github/databrickslabs/blueprint)


Baseline for Databricks Labs projects written in Python. Sources are validated with `mypy`. See [Contributing instructions](CONTRIBUTING.md) if you would like to improve this project.

- [Installation](#installation)
- [Batteries Included](#batteries-included)
  - [Basic Terminal User Interface (TUI) Primitives](#basic-terminal-user-interface-tui-primitives)
    - [Simple Text Questions](#simple-text-questions)
    - [Confirming Actions](#confirming-actions)
    - [Single Choice from List](#single-choice-from-list)
    - [Single Choice from Dictionary](#single-choice-from-dictionary)
    - [Multiple Choices from Dictionary](#multiple-choices-from-dictionary)
    - [Unit Testing Prompts](#unit-testing-prompts)
  - [Nicer Logging Formatter](#nicer-logging-formatter)
    - [Rendering on Dark Background](#rendering-on-dark-background)
    - [Rendering in Databricks Notebooks](#rendering-in-databricks-notebooks)
    - [Integration With Your App](#integration-with-your-app)
    - [Integration with `console_script` Entrypoints](#integration-with-console_script-entrypoints)
  - [Parallel Task Execution](#parallel-task-execution)
    - [Collecting Results](#collecting-results)
    - [Collecting Errors from Background Tasks](#collecting-errors-from-background-tasks)
    - [Strict Failures from Background Tasks](#strict-failures-from-background-tasks)
  - [Building Wheels](#building-wheels)
    - [Released Version Detection](#released-version-detection)
    - [Unreleased Version Detection](#unreleased-version-detection)
    - [Application Name Detection](#application-name-detection)
    - [Install State](#install-state)
    - [Publishing Wheels to Databricks Workspace](#publishing-wheels-to-databricks-workspace)
  - [Databricks CLI's `databricks labs ...` Router](#databricks-clis-databricks-labs--router)
    - [Starting New Projects](#starting-new-projects)
- [Notable Downstream Projects](#notable-downstream-projects)
- [Project Support](#project-support)

# Installation

You can install this project via `pip`:

```
pip install databricks-labs-blueprint
```

# Batteries Included

This library contains a proven set of building blocks, tested in production through [UCX](https://github.com/databrickslabs/ucx) and projects.

## Basic Terminal User Interface (TUI) Primitives

Your command-line apps do need testable interactivity, which is provided by `from databricks.labs.blueprint.tui import Prompts`. Here are some examples of it:

![ucx install](docs/ucx-install.gif)

[[back to top](#databricks-labs-blueprint)]

### Simple Text Questions

Use `prompts.question()` as a bit more involved than `input()` builtin:

```python
from databricks.labs.blueprint.tui import Prompts

prompts = Prompts()
answer = prompts.question('Enter a year', default='2024', valid_number=True)
print(answer)
```

![question](docs/prompts-question.gif)

Optional arguments are:

* `default` (str) - use given value if user didn't input anything
* `max_attempts` (int, default 10) - number of attempts to throw exception after invalid or empty input
* `valid_number` (bool) - input has to be a valid number
* `valid_regex` (bool) - input has to be a valid regular expression
* `validate` - function that takes a string and returns boolean, like `lambda x: 'awesome' in x`, that could be used to further validate input.

[[back to top](#databricks-labs-blueprint)]

### Confirming Actions

Use `prompts.confirm()` to guard any optional or destructive actions of your app:

```python
if prompts.confirm('Destroy database?'):
    print('DESTROYING DATABASE')
```

![confirm](docs/prompts-confirm.gif)

[[back to top](#databricks-labs-blueprint)]

### Single Choice from List

Use to select a value from a list:

```python
answer = prompts.choice('Select a language', ['Python', 'Rust', 'Go', 'Java'])
print(answer)
```

![choice](docs/prompts-choice.gif)

[[back to top](#databricks-labs-blueprint)]

### Single Choice from Dictionary

Use to select a value from the dictionary by showing users sorted dictionary keys:

```python
answer = prompts.choice_from_dict('Select a locale', {
    'Українська': 'ua',
    'English': 'en'
})
print(f'Locale is: {answer}')
```

![choice from dict](docs/prompts-choice-from-dict.gif)

[[back to top](#databricks-labs-blueprint)]

### Multiple Choices from Dictionary

Use to select multiple items from dictionary

```python
answer = prompts.multiple_choice_from_dict(
    'What projects are written in Python? Select [DONE] when ready.', {
    'Databricks Labs UCX': 'ucx',
    'Databricks SDK for Python': 'sdk-py',
    'Databricks SDK for Go': 'sdk-go',
    'Databricks CLI': 'cli',
})
print(f'Answer is: {answer}')
```

![multiple choice](docs/prompts-choice-from-dict.gif)

[[back to top](#databricks-labs-blueprint)]

### Unit Testing Prompts

Use `MockPrompts` with regular expressions as keys and values as answers. The longest key takes precedence.

```python
from databricks.labs.blueprint.tui import MockPrompts

def test_ask_for_int():
    prompts = MockPrompts({r".*": ""})
    res = prompts.question("Number of threads", default="8", valid_number=True)
    assert "8" == res
```

[[back to top](#databricks-labs-blueprint)]

## Nicer Logging Formatter

There's a basic logging configuration available for [Python SDK](https://github.com/databricks/databricks-sdk-py?tab=readme-ov-file#logging), but the default output is not pretty and is relatively inconvenient to read. Here's how make output from Python's standard logging facility more enjoyable to read:

```python
from databricks.labs.blueprint.logger import install_logger

install_logger()

import logging
logging.root.setLevel("DEBUG") # use only for development or demo purposes

logger = logging.getLogger("name.of.your.module")
logger.debug("This is a debug message")
logger.info("This is an table message")
logger.warning("This is a warning message")
logger.error("This is an error message", exc_info=KeyError(123))
logger.critical("This is a critical message")
```

Here are the assumptions made by this formatter:

 * Most likely you're forwarding your logs to a file already, this log formatter is mainly for visual consumption.
 * The average app or Databricks Job most likely finishes running within a day or two, so we display only hours, minutes, and seconds from the timestamp.
 * We gray out debug messages, and highlight all other messages. Errors and fatas are additionally painted with red.
 * We shorten the name of the logger to a readable chunk only, not to clutter the space. Real-world apps have deeply nested folder structures and filenames like `src/databricks/labs/ucx/migration/something.py`, which translate into `databricks.labs.ucx.migration.something` fully-qualified Python module names, that get reflected into `__name__` [top-level code environment](https://docs.python.org/3/library/__main__.html#what-is-the-top-level-code-environment) special variable, that you idiomatically use with logging as `logger.getLogger(__name__)`. This log formatter shortens the full module path to a more readable `d.l.u.migration.something`, which is easier to consume from a terminal screen or a notebook. 
 * We only show the name of the thread if it's other than `MainThread`, because the overwhelming majority of Python applications are single-threaded.

[[back to top](#databricks-labs-blueprint)]

### Rendering on Dark Background

Here's how the output would look like on dark terminal backgrounds, including those from GitHub Actions:

![logger dark](docs/logger-dark.png)

[[back to top](#databricks-labs-blueprint)]

### Rendering in Databricks Notebooks

And here's how things will appear when executed from Databricks Runtime as part of notebook or a workflow:

![logger white](docs/notebook-logger.png)

[[back to top](#databricks-labs-blueprint)]

### Integration With Your App

Just place the following code in your wheel's top-most `__init__.py` file:

```python
from databricks.labs.blueprint.logger import install_logger

install_logger(level="INFO")
```

And place this idiomatic 

```python
# ... insert this into the top of your file
from databricks.labs.blueprint.entrypoint import get_logger

logger = get_logger(__file__)
# ... top of the file insert end
```

... and you'll be able to benefit from the readable console stderr formatting everywhere 

Each time you'd need to turn on debug logging, just invoke `logging.root.setLevel("DEBUG")` (even in notebook).

[[back to top](#databricks-labs-blueprint)]

### Integration with `console_script` Entrypoints

When you invoke Python as an entry point to your wheel (also known as `console_scripts`), [`__name__` top-level code environment](https://docs.python.org/3/library/__main__.html#what-is-the-top-level-code-environment) would always be equal to `__main__`. But you really want to get the logger to be named after your Python module and not just `__main__` (see [rendering in Databricks notebooks](#rendering-in-databricks-notebooks)).

If you create a `dist/logger.py` file with the following contents:

```python
from databricks.labs.blueprint.entrypoint import get_logger, run_main

logger = get_logger(__file__)

def main(first_arg, second_arg, *other):
    logger.info(f'First arg is: {first_arg}')
    logger.info(f'Second arg is: {second_arg}')
    logger.info(f'Everything else is: {other}')
    logger.debug('... and this message is only shown when you are debugging from PyCharm IDE')

if __name__ == '__main__':
    run_main(main)
```

... and invoke it with `python dist/logger.py Hello world, my name is Serge`, you should get back the following output.

```
13:46:42  INFO [dist.logger] First arg is: Hello
13:46:42  INFO [dist.logger] Second arg is: world,
13:46:42  INFO [dist.logger] Everything else is: ('my', 'name', 'is', 'Serge')
```

Everything is made easy thanks to `run_main(fn)` helper.

[[back to top](#databricks-labs-blueprint)]

## Parallel Task Execution

Python applies global interpreter lock (GIL) for compute-intensive tasks, though IO-intensive tasks, like calling Databricks APIs through Databricks SDK for Python, are not subject to GIL. It's quite a common task to perform multiple different API calls in parallel, though it is overwhelmingly difficult to do multi-threading right. `concurrent.futures import ThreadPoolExecutor` is great, but sometimes we want something even more high level. This library helps you navigate the most common road bumps.

[[back to top](#databricks-labs-blueprint)]

### Collecting Results

This library helps you filtering out empty results from background tasks, so that the downstream code is generally simpler. We're also handling the thread pool namind, so that the name of the list of tasks properly gets into log messages. After all background tasks completed their execution, we log something like `Finished 'task group name' tasks: 50% results available (2/4). Took 0:00:00.000604`.

```python
from databricks.labs.blueprint.parallel import Threads

def not_really_but_fine():
    logger.info("did something, but returned None")

def doing_something():
    logger.info("doing something important")
    return f'result from {doing_something.__name__}'

logger.root.setLevel('DEBUG')
tasks = [not_really_but_fine, not_really_but_fine, doing_something, doing_something]
results, errors = Threads.gather("task group name", tasks)

assert ['result from doing_something', 'result from doing_something'] == results
assert [] == errors
```

This will log the following messages:

```
14:20:15 DEBUG [d.l.blueprint.parallel] Starting 4 tasks in 20 threads
14:20:15  INFO [dist.logger][task_group_name_0] did something, but returned None
14:20:15  INFO [dist.logger][task_group_name_1] did something, but returned None
14:20:15  INFO [dist.logger][task_group_name_1] doing something important
14:20:15  INFO [dist.logger][task_group_name_1] doing something important
14:20:15  INFO [d.l.blueprint.parallel][task_group_name_1] task group name 4/4, rps: 7905.138/sec
14:20:15  INFO [d.l.blueprint.parallel] Finished 'task group name' tasks: 50% results available (2/4). Took 0:00:00.000604
```

[[back to top](#databricks-labs-blueprint)]

### Collecting Errors from Background Tasks

Inspired by Go Language's idiomatic error handling approach, this library allows for collecting errors from all of the background tasks and handle them separately. For all other cases, we recommend using [strict failures](#strict-failures-from-background-tasks)

```python
from databricks.sdk.errors import NotFound
from databricks.labs.blueprint.parallel import Threads

def works():
    return True

def fails():
    raise NotFound("something is not right")

tasks = [works, fails, works, fails, works, fails, works, fails]
results, errors = Threads.gather("doing some work", tasks)

assert [True, True, True, True] == results
assert 4 == len(errors)
```

This will log the following messages:

```
14:08:31 ERROR [d.l.blueprint.parallel][doing_some_work_0] doing some work task failed: something is not right: ...
...
14:08:31 ERROR [d.l.blueprint.parallel][doing_some_work_3] doing some work task failed: something is not right: ...
14:08:31 ERROR [d.l.blueprint.parallel] More than half 'doing some work' tasks failed: 50% results available (4/8). Took 0:00:00.001011
```

[[back to top](#databricks-labs-blueprint)]

### Strict Failures from Background Tasks

Use `Threads.strict(...)` to raise `ManyError` with the summary of all failed tasks:

```python
from databricks.sdk.errors import NotFound
from databricks.labs.blueprint.parallel import Threads

def works():
    return True

def fails():
    raise NotFound("something is not right")

tasks = [works, fails, works, fails, works, fails, works, fails]
results = Threads.strict("doing some work", tasks)

# this line won't get executed
assert [True, True, True, True] == results
```

This will log the following messages:

```
...
14:11:46 ERROR [d.l.blueprint.parallel] More than half 'doing some work' tasks failed: 50% results available (4/8). Took 0:00:00.001098
...
databricks.labs.blueprint.parallel.ManyError: Detected 4 failures: NotFound: something is not right
```

[[back to top](#databricks-labs-blueprint)]

## Building Wheels

We recommend deploying applications as wheels. But versioning, testing, and deploying those is often a tedious process.

### Released Version Detection

When you deploy your Python app as a wheel, every time it has to have a different version. This library detects `__about__.py` file automatically anywhere in the project root and reads `__version__` variable from it. We support [SemVer](https://semver.org/) versioning scheme.

```python
from databricks.labs.blueprint.wheels import ProductInfo

product_info = ProductInfo()
version = product_info.released_version()
logger.info(f'Version is: {version}')
```

[[back to top](#databricks-labs-blueprint)]

### Unreleased Version Detection

When you develop your wheel and iterate on testing it, it's often required to upload a file with different name each time you build it. We use `git describe --tags` command to fetch the latest SemVer-compatible tag (e.g. `v0.0.2`) and append the number of commits with timestamp to it. For example, if the released version is `v0.0.1`, then the unreleased version would be something like `0.0.2+120240105144650`. We verify that this version is compatible with both SemVer and [PEP 440](https://peps.python.org/pep-0440/).

```python
product_info = ProductInfo()

version = product_info.unreleased_version()
is_git = product_info.is_git_checkout()
is_unreleased = product_info.is_unreleased_version()

logger.info(f'Version is: {version}')
logger.info(f'Git checkout: {is_git}')
logger.info(f'Is unreleased: {is_unreleased}')
```

[[back to top](#databricks-labs-blueprint)]

### Application Name Detection

Library can infer the name of application by taking the directory name when `__about__.py` file is located within the current project. See [released version detection](#released-version-detection) for more details.

```python
from databricks.labs.blueprint.wheels import ProductInfo

w = WorkspaceClient()
product_info = ProductInfo()
logger.info(f'Product name is: {product_info.product_name()}')
```

[[back to top](#databricks-labs-blueprint)]

### Install State

There always needs to be a location, where you put application code, artifacts, and configuration. This library provides a way to construct this location, which is equal to _/Users/{current.user@example.com}/.{[application_name](#application-name-detection)}/_ on the Databricks Workspace.

```python
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo

w = WorkspaceClient()
product_info = ProductInfo()
state = InstallState(w, product_info.product_name())
install_folder = state.install_folder()

logger.info(f'Install folder is: {install_folder}')
```

[[back to top](#databricks-labs-blueprint)]

### Publishing Wheels to Databricks Workspace

Before you execute a wheel on Databricks, you have to build it and upload it. This library provides detects [released](#released-version-detection) or [unreleased](#unreleased-version-detection) version of the wheel, copies it over to a temporary folder, changes the `__about__.py` file with the right version, and builds the wheel in the temporary location, so that it's not polluted with build artifacts. `Wheels` is a context manager, so it removes all temporary files and folders ather `with` block finishes. This library is successfully used to concurrently test wheels on Shared Databricks Clusters through notebook-scoped libraries.

```python
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.wheels import ProductInfo, Wheels

w = WorkspaceClient()
product_info = ProductInfo()
install_state = InstallState(w, product_info.product_name())

with Wheels(w, install_state, product_info) as wheels:
    remote_wheel = wheels.upload_to_wsfs()
    logger.info(f'Uploaded to {remote_wheel}')
```

This will print something like:

```
15:08:44  INFO [dist.logger] Uploaded to /Users/serge.smertin@databricks.com/.blueprint/wheels/databricks_labs_blueprint-0.0.2+120240105150840-py3-none-any.whl
```

You can also do `wheels.upload_to_dbfs()`, though you're not able to set any access control over it.

[[back to top](#databricks-labs-blueprint)]

## Databricks CLI's `databricks labs ...` Router

This library contains common utilities for Databricks CLI entrypoints defined in [`labs.yml`](labs.yml) file. Here's the example metadata for a tool named `blueprint` with a single `me` command and flag named `--greeting`, that has `Hello` as default value:

```yaml
---
name: blueprint
description: Common libraries for Databricks Labs
install:
  script: src/databricks/labs/blueprint/__init__.py
entrypoint: src/databricks/labs/blueprint/__main__.py
min_python: 3.10
commands:
  - name: me
    description: shows current username
    flags:
     - name: greeting
       default: Hello
       description: Greeting prefix
```

And here's the content for [`src/databricks/labs/blueprint/__main__.py`](src/databricks/labs/blueprint/__main__.py) file, that executes `databricks labs blueprint me` command with `databricks.sdk.WorkspaceClient` automatically injected into an argument with magical name `w`:

```python
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.cli import App

app = App(__file__)
logger = get_logger(__file__)


@app.command
def me(w: WorkspaceClient, greeting: str):
    """Shows current username"""
    logger.info(f"{greeting}, {w.current_user.me().user_name}!")


if "__main__" == __name__:
    app()
```

As you may have noticed, there were only workspace-level commands, but you can also nave native account-level command support. You need to specify the `is_account` property when declaring it in `labs.yml` file:

```yaml
commands:
  # ...
  - name: workspaces
    is_account: true
    description: shows current workspaces
```

and `@app.command(is_account=True)` will get you `databricks.sdk.AccountClient` injected into `a` argument:

```python
from databricks.sdk import AccountClient

@app.command(is_account=True)
def workspaces(a: AccountClient):
    """Shows workspaces"""
    for ws in a.workspaces.list():
        logger.info(f"Workspace: {ws.workspace_name} ({ws.workspace_id})")
```

[[back to top](#databricks-labs-blueprint)]

### Starting New Projects

This tooling makes it easier to start new projects. First, install the CLI:

```
databricks labs install blueprint
```

After, create new project in a designated directory:

```
databricks labs blueprint init-project --target /path/to/folder
```

[[back to top](#databricks-labs-blueprint)]

# Notable Downstream Projects

This library is used in the following projects:

- [UCX - Automated upgrade to Unity Catalog](https://github.com/databrickslabs/ucx)

[[back to top](#databricks-labs-blueprint)]

# Project Support

Please note that this project is provided for your exploration only and is not 
formally supported by Databricks with Service Level Agreements (SLAs). They are 
provided AS-IS, and we do not make any guarantees of any kind. Please do not 
submit a support ticket relating to any issues arising from the use of this project.

Any issues discovered through the use of this project should be filed as GitHub 
[Issues on this repository](https://github.com/databrickslabs/blueprint/issues). 
They will be reviewed as time permits, but no formal SLAs for support exist.