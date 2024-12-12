import os
import logging as pylogging
from google.cloud import logging
from google.cloud.logging.handlers import CloudLoggingHandler

def name():
    return os.getenv("GCP_SERVICE")

class GclClient:

    def __init__(self):
        self.gcl_client = logging.Client.from_service_account_json(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), project=os.getenv("GCP_PROJECT"))
        logger = pylogging.getLogger(name())
        logger.addHandler(CloudLoggingHandler(self.gcl_client, name=name()))
        logger.setLevel(pylogging.INFO)
        self.logger = logger


    def get_logger(self):
        return self.logger



