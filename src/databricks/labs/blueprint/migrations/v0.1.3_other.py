import logging

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def upgrade(ws: WorkspaceClient):
    logger.info(f"something else: {ws.current_user.me().user_name}")
