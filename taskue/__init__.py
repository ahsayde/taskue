import pickle
import sys
import time
from redis import Redis
from uuid import uuid4

from taskue.utils import RedisController
from taskue.task import Task, _Task, TaskStatus, TaskResult, TaskSummary, TASK_DONE_STATES
from taskue.workflow import (
    Workflow,
    _Workflow,
    WorkflowResult,
    WorkflowStatus,
    WORKFLOW_DONE_STATES,
)


class Taskue:
    def __init__(self, redis_conn: Redis, namespace: str = "default"):
        self._namespace = namespace
        self._rctrl = RedisController(redis_conn, namespace=namespace)

    @property
    def namespace(self):
        return self._namespace

    def run(self, stages: list, title: str = None) -> str:
        pipeline = self._rctrl.pipeline()
        workflow = _Workflow(title=title)
        workflow.status = WorkflowStatus.PENDING
        workflow.created_at = time.time()
        workflow.namespace = self.namespace

        for stage, tasks in enumerate(stages):
            workflow.stages.append([])
            for index, task in enumerate(tasks):
                task = _Task(task)
                task.namespace = self.namespace
                task.tid = index
                task.stage = stage
                task.workflow = workflow.uid
                task.status = TaskStatus.CREATED
                task.created_at = workflow.created_at
                workflow.stages[stage].append(TaskSummary(task))
                self._rctrl.save_task(task, queue=True, pipeline=pipeline)

        self._rctrl.save_workflow(workflow, queue=True, pipeline=pipeline)
        pipeline.execute()
        return workflow.uid

    def namespace_list(self):
        return self._rctrl.list_namespaces()

    def namespace_delete(self, namespace):
        self._rctrl.namespace_delete(namespace)

    def runner_list(self):
        return self._rctrl.get_runners()

    def runner_get(self, name):
        runner = self._rctrl.get_runner(name)
        if not runner:
            raise RunnerNotFound()
        return runner

    def workflow_list(self, page=1, limit=25):
        start = (page - 1) * limit
        end = (page * limit) - 1
        for uid in self._rctrl.list_workflows(start, end):
            yield uid.decode()

    def workflow_get(self, uid):
        workflow = self._rctrl.get_workflow(uid)
        if not workflow:
            raise WorkflowNotFound()
        return WorkflowResult(**workflow.__dict__)

    def workflow_wait(self, uid, timeout: int = 60):
        for _ in range(timeout):
            workflow = self.workflow_get(uid)
            if workflow.status in WORKFLOW_DONE_STATES:
                return workflow
            else:
                time.sleep(1)
        else:
            raise TimeoutError("timeout")

    def workflow_delete(self, uid):
        workflow = self.workflow_get(uid)
        if not workflow.is_done:
            raise RuntimeError("Cannot delete unfinished workflow")

        self._rctrl.delete_workflow(uid)

    def task_get(self, uid):
        task = self._rctrl.get_task(uid)
        if not task:
            raise TaskNotFound()
        return TaskResult(task)

    def task_wait(self, uid: str, timeout: int = 60):
        for _ in range(timeout):
            task = self.task_get(uid)
            if task.status in TASK_DONE_STATES:
                return task
            else:
                time.sleep(1)
        else:
            raise TimeoutError("timeout")


class RunnerNotFound(Exception):
    def __str__(self):
        return "runner not found"


class WorkflowNotFound(Exception):
    def __str__(self):
        return "workflow not found"


class TaskNotFound(Exception):
    def __str__(self):
        return "task not found"
