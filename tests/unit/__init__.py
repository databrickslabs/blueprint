import logging

from databricks.labs.blueprint.logger import install_logger

install_logger()

logging.root.setLevel("DEBUG")
