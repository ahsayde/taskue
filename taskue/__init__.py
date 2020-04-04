import logging
import pickle
import sys
import time
from redis import Redis
from uuid import uuid4

LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=LOGGING_FORMAT)


def _decode(data):
    result = dict()
    for key, value in data.items():
        result[key.decode()] = value.decode()
    return result


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


class When:
    always = "always"
    on_success = "on_success"
    on_failure = "on_failure"


class RunnerStatus:
    IDEL = "idel"
    BUSY = "busy"
    STOPPED = "stopped"
    STOPPING = "stopping"
    DEAD = "dead"
    ACTIVE = [IDEL, BUSY]


class WorkflowStatus:
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    DONE = [PASSED, FAILED]


class StageStatus:
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    DONE = [PASSED, FAILED]


class TaskStatus:
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


class _TaskResult:
    def __init__(self):
        self.attempts = 0
        self.rescheduleded = 0
        self.result = None
        self.runner = None
        self.status = TaskStatus.CREATED
        self.created_at = None
        self.queued_at = None
        self.started_at = None
        self.skipped_at = None
        self.terminated_at = None
        self.executed_at = None

    @property
    def is_done(self):
        return self.status in TaskStatus.DONE

    @property
    def is_passed(self):
        return self.status == TaskStatus.PASSED

    @property
    def is_failed(self):
        return self.status in [TaskStatus.FAILED, TaskStatus.TIMEDOUT, TaskStatus.ERRORED, TaskStatus.TERMINATED]


class Task:
    def __init__(
        self,
        title: str = None, 
        stage: int = 1,
        retries: int = 1,
        tag: str = None,
        timeout: int = None,
        allow_failure: bool = False,
        enable_rescheduling: bool = True,
        when: str = When.on_success,
    ):
        self.id = None
        self.title = title or 'Untitled task'
        self.stage = stage
        self.retries = retries
        self.tag = tag
        self.when = when
        self.timeout = timeout
        self.allow_failure = allow_failure
        self.enable_rescheduling = enable_rescheduling
        self.result = None
        self.workflow = None
        self._workload = None

    @property
    def redis_key(self):
        return Rediskey.TASK % self.uid

    @property
    def redis_queue(self):
        return Queue.DEFAULT if not self.tag else Queue.CUSTOM % self.tag

    @property
    def uid(self):
        return "%s_%s_%s" % (self.workflow, self.stage, self.id)

    @property
    def workload(self):
        return pickle.loads(self._workload)

    def execute(self, func, *args, **kwargs):
        self._workload = pickle.dumps((func, args, kwargs))

    def _save(self, connection, notify=False):
        if not (self.workflow and self.stage and self.id):
            raise RuntimeError("Can't save task")

        connection.set(self.redis_key, pickle.dumps(self))
        if notify:
            connection.rpush(Queue.EVENTS, self.uid)


class Workflow:
    def __init__(self, **kwargs):
        self.uid = kwargs.get("uid")
        self.title = kwargs.get("title")
        self.status = kwargs.get("status")
        self.created_at = kwargs.get("created_at")
        self.started_at = kwargs.get("started_at")
        self.done_at = kwargs.get("done_at")
        self.stages = kwargs.get("stages")

    @property
    def is_done(self):
        return self.status in WorkflowStatus.DONE

    @property
    def is_passed(self):
        return self.status == WorkflowStatus.PASSED

    @property
    def is_failed(self):
        return self.status == WorkflowStatus.FAILED


class _Workflow:
    def __init__(self, title: str = None, stages: int = 1):
        self.uid = uuid4().hex[:10]
        self.title = title or "Untitled workflow"
        self.status = WorkflowStatus.PENDING
        self.created_at = time.time()
        self.started_at = None
        self.done_at = None
        self.stages = {}
        self.current_stage = 1
        self._redis_conn = None

    @property
    def redis_key(self):
        return Rediskey.WORKFLOW % self.uid

    @property
    def is_last_stage(self):
        return self.current_stage == len(self.stages.keys())

    @property
    def current_stage_tasks(self):
        return self.stages[self.current_stage].keys()

    def update_task_status(self, task: Task):
        self.stages[task.stage][task.uid] = task.result.status

    def get_task_status(self, task):
        return self.stages[task.stage][task.uid]

    def get_stage_status(self, stage: int):
        if stage > self.current_stage:
            return StageStatus.PENDING

        for status in self.stages[stage].values():
            if status not in TaskStatus.DONE:
                return StageStatus.RUNNING

            if status not in [TaskStatus.PASSED, TaskStatus.SKIPPED]:
                return StageStatus.FAILED
        else:
            return StageStatus.PASSED

    def update_status(self):
        for stage in self.stages.keys():
            status = self.get_stage_status(stage)
            if status not in StageStatus.DONE:
                return

            if status != StageStatus.PASSED:
                self.status = WorkflowStatus.FAILED
                break
        else:
            self.status = WorkflowStatus.PASSED

    def _load_task(self, uid):
        blob = self._redis_conn.get(Rediskey.TASK % uid)
        return pickle.loads(blob)

    def _queue_task(self, task, pipeline):
        task.result.status = TaskStatus.PENDING
        task.result.queued_at = time.time()
        task._save(pipeline)
        pipeline.rpush(task.redis_queue, task.uid)

    def _skip_task(self, task, pipeline):
        task.result.status = TaskStatus.SKIPPED
        task.result.skipped_at = time.time()
        task._save(pipeline)

    def reschedule_task(self, task, pipeline):
        task.result.runner = None
        task.result.rescheduleded += 1
        task.result.queued_at = time.time()
        task._save(pipeline)
        pipeline.lpush(task.redis_queue, task.uid)

    def terminate_task(self, task, pipeline):
        task.result.status = TaskStatus.TERMINATED
        task.result.terminated_at = time.time()
        task._save(pipeline)

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
        for uid in self.current_stage_tasks:
            task = self._load_task(uid)
            if (
                self.current_stage == 1
                or task.when == When.always
                or (prev_status == StageStatus.PASSED and When.on_success)
                or (prev_status != StageStatus.PASSED and task.when == When.on_failure)
            ):
                self._queue_task(task, pipeline)
            else:
                self._skip_task(task, pipeline)

            self.update_task_status(task)
        self.update(pipeline=pipeline)

    def update(self, pipeline=None):
        if not pipeline:
            pipeline = self._redis_conn.pipeline()

        status = self.get_stage_status(self.current_stage)
        if status in StageStatus.DONE:
            if self.is_last_stage:
                self.done_at = time.time()
                self.update_status()
            else:
                self.current_stage += 1
                self.start_next_stage(pipeline, prev_status=status)

        self.save(pipeline)
        pipeline.execute()

    def __getstate__(self):
        self._redis_conn = None
        return self.__dict__


class Taskue:
    def __init__(self, redis_conn: Redis):
        self._redis_conn = redis_conn

    def run(self, stages: list, title: str = None) -> str:
        pipeline = self._redis_conn.pipeline()
        workflow = _Workflow(title=title, stages=len(stages))

        for stage, tasks in enumerate(stages):
            workflow.stages[stage + 1] = dict()
            for index, task in enumerate(tasks):
                task.id = index + 1
                task.stage = stage + 1
                task.workflow = workflow.uid
                task.result = _TaskResult()
                task.result.created_at = workflow.created_at
                workflow.stages[task.stage][task.uid] = task.result.status
                task._save(pipeline)

        workflow.save(pipeline, queue=True)
        pipeline.execute()
        return workflow.uid

    def runner_list(self):
        keys = self._redis_conn.keys(Rediskey.RUNNER % '*')
        for key in keys:
            yield key.decode().replace(Rediskey.RUNNER % '', '')

    def runner_get(self, runner_uid):
        runner = self._redis_conn.hgetall(Rediskey.RUNNER % runner_uid)
        if not runner:
            raise ValueError("Runner %s does not exist" % runner_uid)
        return _decode(runner)

    def workflow_list(self, page=1, limit=25, done_only=False):
        start = (page - 1) * limit
        end = (page * limit) - 1
        for uid in self._redis_conn.zrange(Rediskey.WORKFLOWS, start, end):
            yield uid.decode()

    def workflow_get(self, workflow_uid):
        blob = self._redis_conn.get(Rediskey.WORKFLOW % workflow_uid)
        if not blob:
            raise ValueError("Workflow %s does not exist" % workflow_uid)
        _workflow = pickle.loads(blob)
        return Workflow(** _workflow.__dict__)

    def workflow_delete(self, workflow_uid):
        workflow = self.workflow_get(workflow_uid)
        if not workflow.is_done:
            raise RuntimeError("Cannot delete unfinished workflow")

        pipeline = self._redis_conn.pipeline()
        pipeline.delete(Rediskey.WORKFLOW % workflow_uid)
        pipeline.zrem(Rediskey.WORKFLOWS, workflow_uid)

        tasks = []
        for stage in workflow.stages.values():
            for task_uid in stage.keys():
                tasks.append(Rediskey.TASK % task_uid)

        pipeline.delete(*tasks)
        pipeline.execute()

    def task_get(self, task_uid):
        blob = self._redis_conn.get(Rediskey.TASK % task_uid)
        if not blob:
            raise ValueError("Task %s does not exist" % task_uid)
        return pickle.loads(blob)
