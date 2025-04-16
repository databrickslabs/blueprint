import os

from databricks.sdk.core import with_user_agent_extra

from databricks.labs.blueprint.__about__ import __version__

with_user_agent_extra("blueprint", __version__)

cli_version = os.environ.get("DATABRICKS_CLI_VERSION")
if cli_version:
    with_user_agent_extra("cli", cli_version)
