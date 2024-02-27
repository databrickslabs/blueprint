import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.blueprint.installation import Installation

logger = logging.getLogger(__name__)


def upgrade(installation: Installation, ws: WorkspaceClient):
    logger.info(f"other for: {ws.current_user.me().user_name}")
    list(ws.clusters.list())
