import time
import uuid
import pickle
from taskue.utils import RedisKeys, DEFAULT_QUEUE, logging


class TaskStatus:
    CREATED = "Created"
    QUEUED = "Queued"
    RESCHEDULED = "Rescheduled"
    RUNNING = "Running"
    PASSED = "Passed"
    FAILED = "Failed"
    TIMEOUT = "Timeout"
    SKIPPED = "Skipped"
    UNDONE_STATES = [CREATED, QUEUED, RESCHEDULED, RUNNING]
    DONE_STATES = [PASSED, FAILED, TIMEOUT, SKIPPED]
    FAILED_STATES = [FAILED, TIMEOUT]


class TaskLogHander(logging.Handler):
    def __init__(self, logs):
        logging.Handler.__init__(self)
        self._logs = logs
        self.capture = False

    def emit(self, record):
        if self.capture:
            self._logs.append(
                dict(
                    msg=(record.msg % record.args),
                    level=record.levelname,
                    timestamp=record.created,
                )
            )


class TaskLogger:
    def __init__(self):
        self._logs = []
        self.handler = TaskLogHander(self._logs)

    def get_logs(self):
        return list(self._logs)

    def __enter__(self):
        self.handler.capture = True
        return self

    def __exit__(self, type, value, traceback):
        self.handler.capture = False
        self._logs.clear()


class Task:
    def __init__(
        self,
        stage: int = 1,
        retrires: int = 1,
        timeout: int = 3600,
        skip_on_failure: bool = False,
        tag: str = None,
    ):
        self.uid = uuid.uuid4().hex
        self.stage = stage
        self.retries = retrires
        self.tag = tag or DEFAULT_QUEUE
        self.timeout = timeout
        self.skip_on_failure = skip_on_failure
        self.result = None
        self.error = None
        self.logs = []
        self.created_at = None
        self.started_at = None
        self.queued_at = None
        self.executed_at = None
        self.rescheduled = 0
        self.status = TaskStatus.CREATED
        self.workflow_uid = None
        self.runner_uid = None
        self.__workload = None

    @property
    def is_done(self):
        return self.status in TaskStatus.DONE_STATES

    @property
    def is_passed(self):
        return self.status == TaskStatus.PASSED

    @property
    def is_failed(self):
        return self.status in TaskStatus.FAILED_STATES

    @property
    def rkey(self):
        return RedisKeys.TASK.format(self.uid)

    @property
    def rqueue(self):
        return RedisKeys.QUEUE.format(self.tag)

    @property
    def workload(self):
        return pickle.loads(self.__workload)

    def execute(self, func, *args, **kwargs):
        self.__workload = pickle.dumps((func, args, kwargs))

    def dump(self):
        return pickle.dumps(self)
