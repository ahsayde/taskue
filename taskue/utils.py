import sys
import time
import redis
from loguru import logger


class Rediskey:
    RUNNER = "taskue:runner:%s"
    HEARTBEAT = "taskue:heartbeat:%s"
    WORKFLOW = "taskue:workflow:%s"
    WORKFLOWS = "taskue:workflows"
    TASK = "taskue:task:%s"


class Queue:
    WORKFLOWS = "taskue:queue:workflows"
    EVENTS = "taskue:queue:events"
    DEFAULT = "taskue:queue:default"
    CUSTOM = "taskue:queue:%s"


def _decode(data):
    result = dict()
    for key, value in data.items():
        result[key.decode()] = value.decode()
    return result


logging_format = (
    "<light-blue>{time: YYYY-MM-DD at HH:mm:ss}</> | {extra[app]} | <level>{level}</> | <level>{message}</>"
)
logger.configure(
    handlers=[dict(sink=sys.stderr, format=logging_format, colorize=True),]
)
