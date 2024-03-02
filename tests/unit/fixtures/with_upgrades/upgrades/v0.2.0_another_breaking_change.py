import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.blueprint.installation import Installation

logger = logging.getLogger(__name__)


def upgrade(installation: Installation, ws: WorkspaceClient):
    logger.info(f"breaking change: {ws.current_user.me().user_name} for v0.2.0")
    ws.workspace.delete("/Some/v0.2.0")
