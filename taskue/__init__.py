import pickle
import sys
import time
from redis import Redis
from uuid import uuid4

from taskue.utils import Queue, Rediskey, _decode, logger
from taskue.task import Task, _Task, TaskStatus, TaskResult, TaskSummary, TASK_DONE_STATES
from taskue.workflow import Workflow, _Workflow, WorkflowResult, WorkflowStatus, WORKFLOW_DONE_STATES


class Taskue:
    def __init__(self, redis_conn: Redis):
        self._redis_conn = redis_conn

    def run(self, stages: list, title: str = None) -> str:
        pipeline = self._redis_conn.pipeline()
        workflow = _Workflow(title=title)
        workflow.status = WorkflowStatus.PENDING
        workflow.created_at = time.time()

        for stage, tasks in enumerate(stages):
            workflow.stages.append([])
            for index, task in enumerate(tasks):
                task = _Task(task)
                task.tid = index
                task.stage = stage
                task.workflow = workflow.uid
                task.status = TaskStatus.CREATED
                task.created_at = workflow.created_at
                workflow.stages[stage].append(TaskSummary(task))
                task.save(pipeline)

        workflow.save(pipeline, queue=True)
        pipeline.execute()
        return workflow.uid

    def runner_list(self):
        keys = self._redis_conn.keys(Rediskey.RUNNER % "*")
        for key in keys:
            yield key.decode().replace(Rediskey.RUNNER % "", "")

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
        return WorkflowResult(**pickle.loads(blob).__dict__)

    def wait_for_workflow(self, workflow_uid):
        while True:
            workflow = self.workflow_get(workflow_uid)
            if workflow.status in WORKFLOW_DONE_STATES:
                return workflow
            else:
                time.sleep(1)

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

        return TaskResult(pickle.loads(blob))

    def wait_for_task(self, task_uid):
        while True:
            task = self.task_get(task_uid)
            if task.status in TASK_DONE_STATES:
                return task
            else:
                time.sleep(1)
