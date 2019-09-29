import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class IncorrectNumberOfInputs(Exception):
    def __init__(self, message):
        logger.error(message)
