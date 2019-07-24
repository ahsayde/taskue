import time
import pickle
import uuid
from taskue.utils import logging
from redis import Redis
from taskue.utils import RedisKeys
from taskue.task import Task, TaskStatus


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


class Workflow:
    def __init__(self):
        self.uid = uuid.uuid4().hex
        self.stages = []
        self.current_stage = 1
        self.status = WorkflowStatus.CREATED
        self.created_at = None
        self.queued_at = None
        self.started_at = None
        self.done_at = None

    @property
    def current_stage_tasks(self):
        """
        Gets current stage tasks
        """
        index = self.current_stage - 1
        return self.stages[index].keys()

    @property
    def is_last_stage(self):
        """
        Checks if current stage is the last stage of the workflow
        """
        return self.current_stage == len(self.stages)

    @property
    def rkey(self):
        """
        Returns redis key
        """
        return RedisKeys.WORKFLOW.format(self.uid)

    @property
    def rqueue(self):
        """
        Returns redis queue
        """
        return RedisKeys.WORKFLOWS

    @property
    def is_current_stage_done(self):
        """
        Checks if current stage is done or not
        """
        return self.current_stage_status in WorkflowStageStatus.DONE_STATES

    @property
    def current_stage_status(self):
        """
        Gets current stage status
        """
        return self.get_stage_status(self.current_stage)

    def get_stage_status(self, stage):
        """
        Gets stage status
        
        Arguments:
            stage {int} -- stage number
        """
        index = stage - 1
        states = self.stages[index].values()
        if not set(TaskStatus.UNDONE_STATES).isdisjoint(states):
            return WorkflowStageStatus.RUNNING

        if set(TaskStatus.FAILED_STATES).isdisjoint(states):
            return WorkflowStageStatus.PASSED

        return WorkflowStageStatus.FAILED

    def update_status(self, redis: Redis):
        """
        Updates workflow status
        """
        cached = redis.hgetall(RedisKeys.CACHE.format(self.uid))
        self.update_tasks_status(cached)

        states = []
        for stage, _ in enumerate(self.stages):
            states.append(self.get_stage_status(stage))

        if WorkflowStageStatus.RUNNING in states:
            return

        if WorkflowStageStatus.FAILED in states:
            self.status = WorkflowStatus.FAILED
        else:
            self.status = WorkflowStatus.PASSED

        self.done_at = time.time()

    def add_task(self, task: Task, stage: int = None):
        """
        Adds task to the stage
        
        Arguments:
            task {Task} -- Task object
        
        Keyword Arguments:
            stage {int} -- stage number (default: {None})
        """
        index = stage - 1
        task.stage = stage
        task.created_at = self.created_at
        task.workflow_uid = self.uid
        self.stages[index][task.uid] = task.status

    def add_stage(self, tasks: list = []):
        """
        Adds stage to the workflow
        
        Keyword Arguments:
            tasks {list} -- List of tasks objects (default: {[]})
        """
        self.stages.append({})
        stage = len(self.stages)
        for task in tasks:
            self.add_task(task, stage)

    def dump(self):
        """
        Dumps this object
        """
        return pickle.dumps(self)

    def update_tasks_status(self, cached):
        for stage in self.stages:
            for uid in stage.keys():
                stage[uid] = cached.get(uid.encode("utf-8")).decode()
