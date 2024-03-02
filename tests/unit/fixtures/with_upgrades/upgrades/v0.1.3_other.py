import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.blueprint.installation import Installation

logger = logging.getLogger(__name__)


def upgrade(installation: Installation, ws: WorkspaceClient):
    logger.info(f"something else: {ws.current_user.me().user_name} for v0.1.3")
    ws.workspace.list("/Some/v0.1.3")
