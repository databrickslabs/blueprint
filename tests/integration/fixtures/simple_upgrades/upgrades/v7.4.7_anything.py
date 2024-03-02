import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.blueprint.installation import Installation

logger = logging.getLogger(__name__)


def upgrade(installation: Installation, ws: WorkspaceClient):
    raise ValueError("must not trigger")
