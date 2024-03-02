import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.blueprint.installation import Installation

logger = logging.getLogger(__name__)


def upgrade(installation: Installation, ws: WorkspaceClient):
    logger.info("applying v7.4.7")
    ws.jobs.get(123)
