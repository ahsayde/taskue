import sys
import time
import logging
import uuid
import pickle

DEFAULT_QUEUE = "default"
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=LOGGING_FORMAT)


class RedisKeys:
    PENDING = "tr:pending"
    QUEUE = "tr:queue:{0}"
    TASK_EVENTS = "tr:events"
    RUNNER = "tr:runner:{0}"
    RUNNERS = "tr:runners"
    WORKFLOW = "tr:workflow:{0}"
    TASK = "tr:task:{0}"


class WorkflowStatus:
    CREATED = "Created"
    QUEUED = "Queued"
    RUNNING = "Running"
    PASSED = "Passed"
    FAILED = "Failed"
    DONE_STATES = [PASSED, FAILED]


class WorkflowStageStatus:
    RUNNING = "Running"
    PASSED = "Passed"
    FAILED = "Failed"
    DONE_STATES = [PASSED, FAILED]


class TaskStatus:
    CREATED = "Created"
    QUEUED = "Queued"
    RESCHEDULED = "Rescheduled"
    RUNNING = "Running"
    PASSED = "Passed"
    FAILED = "Failed"
    TIMEOUT = "Timeout"
    SKIPPED = "Skipped"
    DONE_STATES = [PASSED, FAILED, TIMEOUT, SKIPPED]
    FAILED_STATES = [FAILED, TIMEOUT]


class RunnerStatus:
    READY = "Ready"
    BUSY = "Busy"
    DEAD = "Dead"


class RunnerItem:
    def __init__(self, **kwargs):
        self.uid = kwargs.get("uid")
        self.status = kwargs.get("status", RunnerStatus.READY)
        self.task_uid = kwargs.get("task_uid", None)
        self.last_heartbeat = kwargs.get("task_uid", time.time())


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
        retries: int = 1,
        tag: str = DEFAULT_QUEUE,
        timeout: int = 3600,
        skip_on_failure: bool = False,
    ):
        """[summary]
        
        Keyword Arguments:
            stage {int} -- stage number (default: {1})
            retries {int} -- number of retries when task fail (default: {1})
            tag {str} -- taged tasks run only on the runners that have same tag (default: {DEFAULT_QUEUE})
            timeout {int} -- task timeout (default: {3600})
            skip_on_failure {bool} -- skip this task if any task of the previous stage failed (default: {False})
        """
        self.uid = uuid.uuid4().hex
        self.stage = stage
        self.retries = retries
        self.tag = tag
        self.timeout = timeout
        self.skip_on_failure = skip_on_failure
        self.workload = None
        self.result = None
        self.logs = []
        self.created_at = None
        self.started_at = None
        self.queued_at = None
        self.executed_at = None
        self.workflow_uid = None
        self.runner_uid = None
        self.rescheduled = 0
        self.status = TaskStatus.CREATED

    def execute(self, func, *args, **kwargs):
        """[summary]
        
        Arguments:
            func  -- the func to call 
        """
        self.workload = pickle.dumps((func, args, kwargs))


class WorkflowItem:
    def __init__(self, **kwargs):
        self.uid = kwargs.get("uid", None)
        self.stages = kwargs.get("stages", [])
        self.status = kwargs.get("status", WorkflowStatus.CREATED)
        self.current_stage = kwargs.get("current_stage", 1)
        self.created_at = kwargs.get("created_at", None)
        self.started_at = kwargs.get("started_at", None)
        self.done_at = kwargs.get("done_at", None)

    def get_stage_status(self, stage):
        stage_index = stage - 1
        tasks_status = set(self.stages[stage_index].values())
        if TaskStatus.QUEUED in tasks_status or TaskStatus.RUNNING in tasks_status:
            return WorkflowStageStatus.RUNNING

        if set(TaskStatus.FAILED_STATES).isdisjoint(tasks_status):
            return WorkflowStageStatus.PASSED

        return WorkflowStageStatus.FAILED

    @property
    def current_stage_tasks(self):
        stage_index = self.current_stage - 1
        return self.stages[stage_index].keys()

    def update_task_status(self, task):
        stage_index = task.stage - 1
        self.stages[stage_index][task.uid] = task.status

    def is_last_stage(self):
        return self.current_stage == len(self.stages)

    def update_status(self):
        stages_status = []
        for stage, _ in enumerate(self.stages):
            stages_status.append(self.get_stage_status(stage))

        if WorkflowStageStatus.RUNNING in stages_status:
            return

        if WorkflowStageStatus.FAILED in stages_status:
            self.status = WorkflowStatus.FAILED
        else:
            self.status = WorkflowStatus.PASSED


class Workflow:
    def __init__(self, stages: list = []):
        """Workflow class
        
        Keyword Arguments:
            stages {list} -- list of tasks lists ex: [[t1, t2], [t3]] (default: {[]})
        """
        self.uid = uuid.uuid4().hex
        self.status = WorkflowStatus.CREATED
        self._stages = []
        self.stages = stages
        self.created_at = None
        self.started_at = None
        self.done_at = None

    @property
    def stages(self):
        return self._stages

    @stages.setter
    def stages(self, stages):
        self._stages = []
        for stage in stages:
            self.add_stage(stage)

    def add_task(self, task: Task, stage: int = None):
        """Add task to the workflow
        
        Arguments:
            task {Task} -- task object
        
        Keyword Arguments:
            stage {int} -- the stage of the task (default: {None})
        
        Raises:
            StageDoesNotExist: raises if the passed stage doesn't exist
        """
        stage = stage or task.stage
        stage_index = stage - 1

        if stage_index not in range(len(self._stages)):
            raise StageDoesNotExist(stage)

        task.stage = stage
        task.workflow_uid = self.uid
        self._stages[stage_index].append(task)

    def add_stage(self, tasks: list = []):
        """Add stage to the workflow
        
        Arguments:
            tasks {list} -- list of task objects
        
        Returns:
            int -- stage order
        """
        self._stages.append([])
        try:
            stage = len(self._stages)
            for task in tasks:
                self.add_task(task, stage)
        except:
            self._stages.pop()
            raise
        return stage

    def dump(self):
        stages = []
        tasks = []
        for stage in self.stages:
            tasks_dict = {}
            for task in stage:
                task.created_at = self.created_at
                tasks_dict[task.uid] = task.status
                tasks.append(task)
            stages.append(tasks_dict)

        workflow = WorkflowItem(uid=self.uid, stages=stages, created_at=self.created_at)
        return workflow.__dict__, tasks


def decode_hash(hashobj):
    decoded = dict()
    for i in range(int(len(hashobj) / 2)):
        value = hashobj.pop().decode()
        key = hashobj.pop().decode()
        decoded[key] = value
    return decoded


class StageDoesNotExist(Exception):
    def __init__(self, stage):
        self.stage = stage

    def __str__(self):
        return "Stage {} doesn't exist, create it first".format(self.stage)
