import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.blueprint.installation import Installation

logger = logging.getLogger(__name__)


def upgrade(installation: Installation, ws: WorkspaceClient):
    logger.info(f"upgrading user: {ws.current_user.me().user_name} to v0.0.1")
    list(ws.jobs.list())
