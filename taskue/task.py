import inspect
import os
import pickle
import time
import uuid
import json
import inspect
from enum import Enum

__all__ = ("Task", "TaskStatus", "TaskSummary", "TaskResult", "Conditions")


class Conditions(str, Enum):
    """ Task execution conditions """

    ALWAYS = "always"
    ON_SUCCESS = "on_success"
    ON_FAILURE = "on_failure"


class TaskStatus(str, Enum):
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
        self._uid = kwargs.get("_uid", None)
        self._tid = kwargs.get("_tid", 0)
        self._title = kwargs.get("_title", "Untitled Task")
        self._retries = kwargs.get("_retries", 1)
        self._tag = kwargs.get("_tag", None)
        self._when = kwargs.get("_when", Conditions.ALWAYS)
        self._timeout = kwargs.get("_timeout", 3600)
        self._allow_failure = kwargs.get("_allow_failure", False)
        self._reschedule = kwargs.get("_reschedule", True)
        self._stage = kwargs.get("_stage", 1)
        self._workload = kwargs.get("_workload", None)
        self._workload_info = kwargs.get("_workload_info", dict())
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
    def json(self):
        ddict = self.__dict__.copy()
        ddict.pop("_workload")
        return ddict

    @property
    def uid(self):
        return self._uid

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
    def reschedule(self):
        return self._reschedule

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
        reschedule: bool = True,
        when: str = Conditions.ON_SUCCESS,
    ):
        Base.__init__(
            self,
            _title=title,
            _retries=retries,
            _tag=tag,
            _timeout=timeout,
            _allow_failure=allow_failure,
            _reschedule=reschedule,
            _when=when,
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

    @Base.reschedule.setter  # pylint: disable=no-member
    def reschedule(self, reschedule: bool):
        """ Sets task's reschedule flag """
        self._reschedule = reschedule

    @Base.when.setter  # pylint: disable=no-member
    def when(self, when: Conditions):
        """ Sets task's execution condition """
        self._when = when

    def execute(self, func: callable, *args, **kwargs):
        """ Sets task's workload """
        module = inspect.getmodule(func)
        self._workload = pickle.dumps((func, args, kwargs))
        self._workload_info["module_name"] = module.__name__
        self._workload_info["module_path"] = getattr(module, "__file__", None)

        


class TaskResult(Base):
    """ Task result class """

    def __init__(self, task: Task):
        super().__init__(**task.__dict__)

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
        super().__init__(**task.__dict__)
        self.uid = task.uid or uuid.uuid4().hex[:10]
        self.status = task.status or TaskStatus.CREATED
        self.created_at = task.created_at or time.time()

    @property
    def tid(self):
        return self._tid

    @property
    def workload_info(self):
        return self._workload_info

    @tid.setter
    def tid(self, value):
        self._tid = value

    @Base.uid.setter  # pylint: disable=no-member
    def uid(self, value):
        self._uid = value

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

    def queue(self, rctrl, pipeline=None):
        self.status = TaskStatus.PENDING
        self.queued_at = time.time()
        self.save(rctrl, queue=True, pipeline=pipeline)

    def skip(self, rctrl, pipeline=None):
        self.status = TaskStatus.SKIPPED
        self.skipped_at = time.time()
        self.save(rctrl, pipeline=pipeline)

    def reschedule(self, rctrl, pipeline=None):
        self.runner = None
        self.rescheduleded += 1
        self.queued_at = time.time()
        self.save(rctrl, pipeline=pipeline)

    def terminate(self, rctrl, pipeline):
        self.status = TaskStatus.TERMINATED
        self.terminated_at = time.time()
        self.save(rctrl, pipeline=pipeline)

    def save(self, rctrl, queue=False, notify=False, pipeline=None):
        rctrl.task_save(self, notify=notify, queue=queue, pipeline=pipeline)


class TaskSummary:
    """ Task summary  """

    def __init__(self, task: (Task, _Task)):
        self.uid = task.uid
        self.title = task.title
        self.status = task.status
        self.stage = task.stage
        self.allow_failure = task.allow_failure
        self.rescheduleded = task.rescheduleded > 1
