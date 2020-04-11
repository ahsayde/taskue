import pickle
import time
from enum import Enum

from taskue.utils import Queue, Rediskey


__all__ = ("Task", "TaskStatus", "TaskSummary", "TaskResult", "Conditions")


class Conditions(Enum):
    """ Task execution conditions """

    ALWAYS = "always"
    ON_SUCCESS = "on_success"
    ON_FAILURE = "on_failure"


class TaskStatus(Enum):
    """ Task states """

    CREATED = "created"
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    TERMINATED = "terminated"
    ERRORED = "errored"
    SKIPPED = "skipped"
    TIMEDOUT = "timedout"
    DONE = [PASSED, FAILED, ERRORED, TIMEDOUT, SKIPPED, TERMINATED]


TASK_DONE_STATES = [
    TaskStatus.PASSED,
    TaskStatus.FAILED,
    TaskStatus.ERRORED,
    TaskStatus.TIMEDOUT,
    TaskStatus.SKIPPED,
    TaskStatus.TERMINATED,
]


class Base:
    """ Base task class """

    def __init__(self, *args, **kwargs):
        self._title = kwargs.get("_title", "Untitled Task")
        self._retries = kwargs.get("_retries", 1)
        self._tag = kwargs.get("_tag", None)
        self._when = kwargs.get("_when", Conditions.ALWAYS)
        self._timeout = kwargs.get("_timeout", 3600)
        self._allow_failure = kwargs.get("_allow_failure", False)
        self._enable_rescheduling = kwargs.get("_enable_rescheduling", True)
        self._workflow = kwargs.get("workflow", None)
        self._tid = kwargs.get("_tid", None)
        self._stage = kwargs.get("_stage", 1)
        self._workload = kwargs.get("_workload", None)
        self._attempts = kwargs.get("_attempts", 0)
        self._rescheduleded = kwargs.get("_rescheduleded", 0)
        self._result = kwargs.get("_result", None)
        self._runner = kwargs.get("_runner", None)
        self._workflow = kwargs.get("_workflow", None)
        self._status = kwargs.get("_status", None)
        self._created_at = kwargs.get("_created_at", None)
        self._queued_at = kwargs.get("_queued_at", None)
        self._started_at = kwargs.get("_started_at", None)
        self._skipped_at = kwargs.get("_skipped_at", None)
        self._terminated_at = kwargs.get("_terminated_at", None)
        self._executed_at = kwargs.get("_executed_at", None)

    @property
    def title(self):
        return self._title

    @property
    def retries(self):
        return self._retries

    @property
    def tag(self):
        return self._tag

    @property
    def when(self):
        return self._when

    @property
    def timeout(self):
        return self._timeout

    @property
    def allow_failure(self):
        return self._allow_failure

    @property
    def enable_rescheduling(self):
        return self._enable_rescheduling

    @property
    def tid(self):
        return self._tid

    @property
    def uid(self):
        return "%s_%s_%s" % (self.workflow, self.stage, self.tid)

    @property
    def attempts(self):
        return self._attempts

    @property
    def stage(self):
        return self._stage

    @property
    def workload(self):
        if self._workload is not None:
            return pickle.loads(self._workload)

    @property
    def rescheduleded(self):
        return self._rescheduleded

    @property
    def result(self):
        return self._result

    @property
    def runner(self):
        return self._runner

    @property
    def workflow(self):
        return self._workflow

    @property
    def status(self):
        return self._status

    @property
    def created_at(self):
        return self._created_at

    @property
    def queued_at(self):
        return self._queued_at

    @property
    def started_at(self):
        return self._started_at

    @property
    def skipped_at(self):
        return self._skipped_at

    @property
    def terminated_at(self):
        return self._terminated_at

    @property
    def executed_at(self):
        return self._executed_at

    @property
    def is_done(self):
        return self.status in TASK_DONE_STATES


class Task(Base):
    """ External task class """

    def __init__(
        self,
        title: str = None,
        retries: int = 1,
        tag: str = None,
        timeout: int = None,
        allow_failure: bool = False,
        enable_rescheduling: bool = True,
        when: str = Conditions.ON_SUCCESS,
    ):
        Base.__init__(
            self,
            title=title,
            retries=retries,
            tag=tag,
            timeout=timeout,
            allow_failure=allow_failure,
            enable_rescheduling=enable_rescheduling,
            when=when,
        )

    @Base.title.setter  # pylint: disable=no-member
    def title(self, title: str):
        """ Sets task's title """
        self._title = title

    @Base.retries.setter  # pylint: disable=no-member
    def retries(self, retries: int):
        """ Sets task's retries """
        self._retries = retries

    @Base.tag.setter  # pylint: disable=no-member
    def tag(self, tag: str):
        """ Sets task's tag """
        self._tag = tag

    @Base.timeout.setter  # pylint: disable=no-member
    def timeout(self, timeout: int):
        """ Sets task's timeout """
        self._timeout = timeout

    @Base.allow_failure.setter  # pylint: disable=no-member
    def allow_failure(self, allow_failure: bool):
        """ Sets task's allow_failure flag """
        self._allow_failure = allow_failure

    @Base.enable_rescheduling.setter  # pylint: disable=no-member
    def enable_rescheduling(self, enable_rescheduling: bool):
        """ Sets task's enable_rescheduling flag """
        self._enable_rescheduling = enable_rescheduling

    @Base.when.setter  # pylint: disable=no-member
    def when(self, when: Conditions):
        """ Sets task's execution condition """
        self._when = when

    def execute(self, func, *args, **kwargs):
        """ Sets task's workload """
        self._workload = pickle.dumps((func, args, kwargs))


class TaskResult(Base):
    """ Task result class """

    def __init__(self, task: Task):
        super().__init__(** task.__dict__)

    @property
    def is_passed(self):
        return self.status == TaskStatus.PASSED

    @property
    def is_failed(self):
        return self.status in [
            TaskStatus.FAILED,
            TaskStatus.TIMEDOUT,
            TaskStatus.ERRORED,
            TaskStatus.TERMINATED,
        ]


class _Task(Base):
    """ Internal task class """

    def __init__(self, task: Task):
        super().__init__(** task.__dict__)

    @property
    def redis_key(self):
        return Rediskey.TASK % self.uid

    @property
    def redis_queue(self):
        return Queue.DEFAULT if not self.tag else Queue.CUSTOM % self.tag

    @Base.tid.setter  # pylint: disable=no-member
    def tid(self, value):
        self._tid = value

    @Base.stage.setter  # pylint: disable=no-member
    def stage(self, value):
        self._stage = value

    @Base.attempts.setter  # pylint: disable=no-member
    def attempts(self, value):
        self._attempts = value

    @Base.rescheduleded.setter  # pylint: disable=no-member
    def rescheduleded(self, value):
        self._rescheduleded = value

    @Base.result.setter  # pylint: disable=no-member
    def result(self, value):
        self._result = value

    @Base.runner.setter  # pylint: disable=no-member
    def runner(self, value):
        self._runner = value

    @Base.workflow.setter  # pylint: disable=no-member
    def workflow(self, value):
        self._workflow = value

    @Base.status.setter  # pylint: disable=no-member
    def status(self, value):
        self._status = value

    @Base.created_at.setter  # pylint: disable=no-member
    def created_at(self, value):
        self._created_at = value

    @Base.queued_at.setter  # pylint: disable=no-member
    def queued_at(self, value):
        self._queued_at = value

    @Base.started_at.setter  # pylint: disable=no-member
    def started_at(self, value):
        self._started_at = value

    @Base.skipped_at.setter  # pylint: disable=no-member
    def skipped_at(self, value):
        self._skipped_at = value

    @Base.terminated_at.setter  # pylint: disable=no-member
    def terminated_at(self, value):
        self._terminated_at = value

    @Base.executed_at.setter  # pylint: disable=no-member
    def executed_at(self, value):
        self._executed_at = value

    def queue(self):
        self.status = TaskStatus.PENDING
        self.queued_at = time.time()

    def skip(self):
        self.status = TaskStatus.SKIPPED
        self.skipped_at = time.time()

    def reschedule(self):
        self.runner = None
        self.rescheduleded += 1
        self.queued_at = time.time()

    def terminate(self):
        self.status = TaskStatus.TERMINATED
        self.terminated_at = time.time()

    def save(self, connection, notify=False):
        connection.set(self.redis_key, pickle.dumps(self))
        if notify:
            connection.rpush(Queue.EVENTS, self.uid)


class TaskSummary:
    """ Task summary class """

    def __init__(self, task: (Task, _Task)):
        self.uid = task.uid
        self.title = task.title
        self.status = task.status
        self.allow_failure = task.allow_failure
        self.rescheduleded = task.rescheduleded > 1