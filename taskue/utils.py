import sys
import logging


DEFAULT_QUEUE = "default"
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=LOGGING_FORMAT)


class RedisKeys:
    WORKFLOWS = "tsku:workflows"
    QUEUE = "tsku:queue:{0}"
    CACHE = "tsku:cache:{0}"
    EVENTS = "tsku:events"
    RUNNER = "tsku:runner:{0}"
    RUNNERS = "tsku:runners"
    WORKFLOW = "tsku:workflow:{0}"
    TASK = "tsku:task:{0}"
