from databricks.sdk.core import with_user_agent_extra

from .__about__ import __version__

with_user_agent_extra("blueprint", __version__)
