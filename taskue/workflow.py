import pickle
import time
import uuid
from enum import Enum

from taskue.utils import Queue, Rediskey
from taskue.task import _Task, TaskStatus, TaskSummary, Conditions, TASK_DONE_STATES


__all__ = ("Workflow", "WorkflowStatus", "StageStatus", "WorkflowResult")


class WorkflowStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    DONE = [PASSED, FAILED]


class StageStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    DONE = [PASSED, FAILED]


WORKFLOW_DONE_STATES = [WorkflowStatus.PASSED, WorkflowStatus.FAILED]
STAGE_DONE_STATES = [StageStatus.PASSED, StageStatus.FAILED]


class Base:
    """ Base workflow class """

    def __init__(self, *args, **kwargs):
        self._uid = kwargs.get("_uid", uuid.uuid4().hex[:10])
        self._title = kwargs.get("_title", "Untitled Workflow")
        self._status = kwargs.get("_status", None)
        self._created_at = kwargs.get("_created_at", None)
        self._started_at = kwargs.get("_started_at", None)
        self._done_at = kwargs.get("_done_at", None)
        self._stages = kwargs.get("_stages", [])
        self._current_stage = kwargs.get("_current_stage", 0)
        self._redis_conn = None

    @property
    def uid(self):
        return self._uid

    @property
    def title(self):
        return self._title

    @property
    def status(self):
        return self._status

    @property
    def stages(self):
        return self._stages

    @property
    def created_at(self):
        return self._created_at

    @property
    def started_at(self):
        return self._started_at

    @property
    def done_at(self):
        return self._done_at

    @property
    def is_done(self):
        return self.status in WORKFLOW_DONE_STATES


class WorkflowResult(Base):
    """ Workflow result class """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def is_passed(self):
        return self.status == WorkflowStatus.PASSED

    @property
    def is_failed(self):
        return self.status == WorkflowStatus.FAILED


class Workflow(Base):
    """ Workflow external class """

    def __init__(self, title: str = None):
        super().__init__(title=title)

    @Base.title.setter  # pylint: disable=no-member
    def title(self, value):
        self._title = value


class _Workflow(Base):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def redis_key(self):
        return Rediskey.WORKFLOW % self.uid

    @property
    def redis_conn(self, value):
        self._redis_conn = value

    @property
    def is_last_stage(self):
        return self.current_stage == len(self.stages) - 1

    @property
    def stages(self):
        return self._stages

    @property
    def current_stage_tasks(self):
        return self.stages[self.current_stage]

    @property
    def current_stage(self):
        return self._current_stage

    @current_stage.setter
    def current_stage(self, value):
        self._current_stage = value

    @redis_conn.setter
    def redis_conn(self, value):
        self._redis_conn = value

    @Base.status.setter  # pylint: disable=no-member
    def status(self, value):
        self._status = value

    @stages.setter  # pylint: disable=no-member
    def stages(self, value):
        self._stages = value

    @Base.created_at.setter  # pylint: disable=no-member
    def created_at(self, value):
        self._created_at = value

    @Base.started_at.setter  # pylint: disable=no-member
    def started_at(self, value):
        self._started_at = value

    @Base.done_at.setter  # pylint: disable=no-member
    def done_at(self, value):
        self._done_at = value

    def update_task(self, task: _Task):
        self.stages[task.stage][task.tid] = TaskSummary(task)

    def get_task_status(self, task):
        return self.stages[task.stage][task.tid].status

    def get_stage_status(self, stage: int):
        if stage > self.current_stage:
            return StageStatus.PENDING

        for task in self.stages[stage]:
            if task.status not in TASK_DONE_STATES:
                return StageStatus.RUNNING

            if task.status not in [TaskStatus.PASSED, TaskStatus.SKIPPED] and not task.allow_failure:
                return StageStatus.FAILED
        else:
            return StageStatus.PASSED

    def update_status(self):
        for stage in range(len(self.stages)):
            status = self.get_stage_status(stage)
            if status not in STAGE_DONE_STATES:
                return

            if status != StageStatus.PASSED:
                self.status = WorkflowStatus.FAILED
                break
        else:
            self.status = WorkflowStatus.PASSED

    def load_task(self, uid):
        blob = self._redis_conn.get(Rediskey.TASK % uid)
        return pickle.loads(blob)

    def queue_task(self, task, pipeline):
        task.queue()
        task.save(pipeline)
        pipeline.rpush(task.redis_queue, task.uid)

    def skip_task(self, task, pipeline):
        task.skip()
        task.save(pipeline)

    def reschedule_task(self, task, pipeline):
        task.reschedule()
        task.save(pipeline)
        pipeline.lpush(task.redis_queue, task.uid)

    def terminate_task(self, task, pipeline):
        task.terminate()
        task.save(pipeline)

    def save(self, pipeline, queue=False):
        pipeline.set(Rediskey.WORKFLOW % self.uid, pickle.dumps(self))
        if queue:
            pipeline.zadd(Rediskey.WORKFLOWS, self.uid, self.created_at)
            pipeline.rpush(Queue.WORKFLOWS, self.uid)

    def start(self):
        pipeline = self._redis_conn.pipeline()
        self.status = WorkflowStatus.RUNNING
        self.started_at = time.time()
        self.start_next_stage(pipeline=pipeline)

    def start_next_stage(self, pipeline, prev_status=None):
        for task in self.current_stage_tasks:
            task = self.load_task(task.uid)
            if (
                self.current_stage == 1
                or task.when == Conditions.ALWAYS
                or (prev_status == StageStatus.PASSED and Conditions.ON_SUCCESS)
                or (prev_status != StageStatus.PASSED and task.when == Conditions.ON_FAILURE)
            ):
                self.queue_task(task, pipeline)
            else:
                self.skip_task(task, pipeline)

            self.update_task(task)
        self.update(pipeline=pipeline)

    def update(self, pipeline=None):
        if not pipeline:
            pipeline = self._redis_conn.pipeline()

        status = self.get_stage_status(self.current_stage)
        if status in STAGE_DONE_STATES:
            if self.is_last_stage:
                self.done_at = time.time()
                self.update_status()
            else:
                self.current_stage += 1
                self.start_next_stage(pipeline, prev_status=status)

        self.save(pipeline)
        pipeline.execute()

    def __getstate__(self):
        self.redis_conn = None
        return self.__dict__