import time
import pickle
import sys
from redis import Redis
from taskue.utils import RedisKeys, logging
from taskue.task import Task, TaskStatus
from taskue.workflow import Workflow, WorkflowStatus


class Taskue:
    def __init__(self, redis_conn: Redis):
        self._redis = redis_conn

    def _queue(self, obj):
        self._redis.rpush(obj.rqueue, obj.uid)

    def _save(self, obj):
        if isinstance(obj, Task) and obj.workflow_uid:
            self._cache_task_status(obj)
        self._redis.set(obj.rkey, obj.dump())

    def _cache_task_status(self, task):
        self._redis.hmset(
            RedisKeys.CACHE.format(task.workflow_uid), {task.uid: task.status}
        )

    def run(self, task: Task):
        """
        Runs signle task
        
        Arguments:
            task {Task} -- Task object
        
        Returns:
            str -- Task uid
        """
        task.status = TaskStatus.QUEUED
        task.created_at = task.queued_at = time.time()
        self._save(task)
        self._queue(task)
        return task.uid

    def run_workflow(self, stages):
        """
        Run workflow
        
        Arguments:
            stages {list} -- List of tasks objects
        
        Returns:
            str -- Workflow uid
        """
        workflow = Workflow()
        workflow.created_at = workflow.queued_at = time.time()
        workflow.status = WorkflowStatus.QUEUED

        for idx, stage in enumerate(stages):
            workflow.add_stage()
            for task in stage:
                workflow.add_task(task, idx + 1)
                self._save(task)

        self._save(workflow)
        self._queue(workflow)
        return workflow.uid

    def get_task_info(self, uid):
        """
        Gets task info
        
        Arguments:
            uid {str} -- Task uid

        Returns:
            Task -- Task object
        """
        blob = self._redis.get(RedisKeys.TASK.format(uid))
        if not blob:
            raise TaskNotFound(uid)
        return pickle.loads(blob)

    def get_workflow_info(self, uid):
        """
        Gets workflow info
        
        Arguments:
            uid {str} -- Workflow uid
        
        Returns:
            Workflow -- Workflow object
        """
        blob = self._redis.get(RedisKeys.WORKFLOW.format(uid))
        if not blob:
            raise WorkflowNotFound(uid)
        return pickle.loads(blob)

    def wait_for_task(self, uid, timeout=300):
        """
        Waits for task to finish
        
        Arguments:
            uid {str} -- Task uid
        
        Keyword Arguments:
            timeout {int} -- timeout in seconds (default: {300})
        """
        for _ in range(timeout):
            task = self.get_task_info(uid)
            if task.status in TaskStatus.DONE_STATES:
                return task
            else:
                time.sleep(1)
        else:
            raise TimeoutException("Task", uid, timeout)

    def wait_for_workflow(self, uid, timeout=300):
        """
        Waits for workflow to finish
        
        Arguments:
            uid {str} -- Workflow uid
        
        Keyword Arguments:
            timeout {int} -- timeout in seconds (default: {300})
        """
        for _ in range(timeout):
            workflow = self.get_workflow_info(uid)
            if workflow.status in WorkflowStatus.DONE_STATES:
                return workflow
            else:
                time.sleep(1)
        else:
            raise TimeoutException("Workflow", uid, timeout)

    def list_tasks(self):
        """
        List tasks
        
        Returns:
            iter -- tasks iterator
        """
        return self._redis.scan_iter(RedisKeys.TASK.format("*"))

    def list_workflows(self):
        """
        List workflows
        
        Returns:
            iter -- workflows iterator
        """
        return self._redis.scan_iter(RedisKeys.WORKFLOW.format("*"))

    def delete_task(self, uid):
        """
        Deletes task
        
        Arguments:
            uid {str} -- Task uid
        """
        task = self.get_task_info(uid)
        if task.workflow_uid:
            raise RuntimeError()
        self._redis.delete(task.rkey)

    def delete_workflow(self, uid):
        """
        Deletes workflow
        
        Arguments:
            uid {str} -- Workflow uid
        """
        workflow = self.get_workflow_info(uid)
        for stage in workflow.stages:
            for task_uid in stage:
                self._redis.delete(RedisKeys.TASK.format(task_uid))
        self._redis.delete(workflow.rkey)


class TaskNotFound(Exception):
    def __init__(self, uid):
        self.uid = uid

    def __str__(self):
        return "Task {} not found".format(self.uid)


class WorkflowNotFound(Exception):
    def __init__(self, uid):
        self.uid = uid

    def __str__(self):
        return "Workflow {} not found".format(self.uid)


class TimeoutException(Exception):
    def __init__(self, resource, uid, timeout):
        self.uid = uid
        self.resource = resource
        self.timeout = timeout

    def __str__(self):
        return "{} {} did not finish after {} seconds".format(
            self.resource, self.uid, self.timeout
        )
