import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.blueprint.installation import Installation

logger = logging.getLogger(__name__)


def upgrade(installation: Installation, ws: WorkspaceClient):
    installation.upload("v0.2.0", b"...")
