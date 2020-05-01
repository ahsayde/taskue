import pickle
import sys
import time
from typing import Iterator, Sequence, Union

from loguru import logger
from redis import Redis
from taskue.task import TASK_DONE_STATES, Conditions, Task, TaskResult, TaskStatus, TaskSummary, _Task
from taskue.controller import RedisController
from taskue.workflow import WORKFLOW_DONE_STATES, WorkflowResult, WorkflowStatus, _Workflow


logging_format = (
    "<light-blue>{time: YYYY-MM-DD at HH:mm:ss}</> | {extra[app]} | <level>{level}</> | <level>{message}</>"
)
logger.configure(
    handlers=[dict(sink=sys.stderr, format=logging_format, colorize=True)]
)


class Taskue:
    def __init__(self, redis_conn: Redis, namespace: str = "default"):
        """Taskue client

        Args:
            redis_conn (Redis): redis connection
            namespace (str, optional): namespace. Defaults to "default".
        """
        self._namespace = namespace
        self._ctrl = RedisController(redis_conn, namespace=namespace)

    @property
    def namespace(self):
        """Current namespace
        """
        return self._namespace

    def enqueue(self, work: Union[Task, Sequence[Sequence[Task]]], title: str = "") -> str:
        """Enqueue task or multiple tasks as a workflow

        Args:
            work (Union[Task, Sequence[Sequence[Task]]]): task or list of lists of tasks.
            title (str, optional): task / workflow title. Defaults to "".

        Returns:
            str: task / workflow unique id
        """
        title = title or "Untitled"
        if isinstance(work, Task):
            return self._enqueue_task(work)

        elif isinstance(work, list):
            return self._enqueue_workflow(work)

    def _enqueue_task(self, task: Task, title: str = None) -> str:
        task = _Task(task)
        task.queue(self._ctrl)
        return task.uid

    def _enqueue_workflow(self, stages: Sequence[Sequence[Task]], title: str = None):
        pipeline = self._ctrl.pipeline()
        workflow = _Workflow()
        workflow.title = title

        for stage, tasks in enumerate(stages):
            workflow.stages.append([])
            for index, task in enumerate(tasks):
                task = _Task(task)
                task.tid = index
                task.stage = stage
                task.workflow = workflow.uid
                workflow.stages[stage].append(TaskSummary(task))
                task.save(self._ctrl, pipeline=pipeline)

        workflow.save(self._ctrl, queue=True, pipeline=pipeline)
        pipeline.execute()
        return workflow.uid

    def namespace_list(self) -> Sequence[dict]:
        """List namespaces

        Returns:
            Sequence[dict]: list of namespaces
        """
        namespaces = self._ctrl.namespaces_list()
        return list(namespaces)

    def namespace_delete(self, namespace: str):
        """Delete namespace

        Args:
            namespace (str): namespace name
        """
        self._ctrl.namespace_delete(namespace)

    def runner_list(self) -> Sequence[dict]:
        """List runners

        Returns:
            Sequence[dict]: list of runners
        """
        runners = self._ctrl.runners_list()
        return list(runners)

    def workflow_list(self, page: int = 1, limit: int = 25) -> Iterator[WorkflowResult]:
        """List workflows

        Args:
            page (int, optional): page number. Defaults to 1.
            limit (int, optional): number of results per page. Defaults to 25.

        Yields:
            WorkflowResult: workflow result
        """
        start = (page - 1) * limit
        end = (page * limit) - 1
        for workflow in self._ctrl.workflows_list(start, end):
            yield WorkflowResult(workflow)

    def workflow_get(self, workflow_uid: str) -> WorkflowResult:
        """Get workflow result

        Args:
            workflow_uid (str): workflow unique id.

        Raises:
            NotFound: raises if workflow is not found.

        Returns:
            WorkflowResult: workflow result.
        """
        workflow = self._ctrl.workflow_get(workflow_uid)
        if not workflow:
            raise NotFound("workflow %s not found" % workflow_uid)
        return WorkflowResult(workflow)

    def workflow_wait(self, workflow_uid: str, timeout: int = 60) -> WorkflowResult:
        """Wait until workflow finish

        Args:
            workflow_uid (str): workflow unique id.
            timeout (int, optional): timeout in seconds. Defaults to 60.

        Raises:
            Timeout: raises if timeout is exceeded

        Returns:
            WorkflowResult: workflow result
        """
        for _ in range(timeout):
            workflow = self.workflow_get(workflow_uid)
            if workflow.status in WORKFLOW_DONE_STATES:
                return workflow
            else:
                time.sleep(1)
        else:
            raise Timeout("timeout exceeded")

    def workflow_delete(self, workflow_uid: str):
        """Delete workflow

        Args:
            workflow_uid (str): workflow unique id.

        Raises:
            InvalidAction: raises if workflow is not finished yet.
        """
        workflow = self.workflow_get(workflow_uid)
        if not workflow.is_done:
            raise InvalidAction("Cannot delete unfinished workflow")

        self._ctrl.workflow_delete(workflow_uid)

    def task_list(self, page: int = 1, limit: int = 25) -> Iterator[TaskResult]:
        """List tasks

        Args:
            page (int, optional): page number. Defaults to 1.
            limit (int, optional): number of results per page. Defaults to 25.

        Yields:
            TaskResult: task result
        """
        start = (page - 1) * limit
        end = (page * limit) - 1
        for task in self._ctrl.tasks_list(start, end):
            yield TaskResult(task)

    def task_get(self, task_uid: str) -> TaskResult:
        """Get task result

        Args:
            task_uid (str): task unique id.

        Raises:
            NotFound: if task is not found.

        Returns:
            TaskResult: task result
        """
        task = self._ctrl.task_get(task_uid)
        if not task:
            raise NotFound("task %s not found" % task_uid)
        return TaskResult(task)

    def task_wait(self, task_uid: str, timeout: int = 60) -> TaskResult:
        """Wait until the task finish

        Args:
            task_uid (str): task unique id.
            timeout (int, optional): timeout in seconds. Defaults to 60.

        Raises:
            Timeout: if timeout is exceeded.

        Returns:
            TaskResult: task result.
        """
        for _ in range(timeout):
            task = self.task_get(task_uid)
            if task.status in TASK_DONE_STATES:
                return task
            else:
                time.sleep(1)
        else:
            raise Timeout("timeout exceeded")

    def task_delete(self, task_uid: str):
        """Delete task by its unique id

        Args:
            task_uid (str): task unique id.

        Raises:
            InvalidAction: raises if task is a part of a workflow.
        """
        task = self.task_get(task_uid)
        if task.workflow:
            raise InvalidAction("Cannot delete task which is a part of workflow")

        self._ctrl.task_delete(task_uid)


class NotFound(Exception):
    pass


class InvalidAction(Exception):
    pass


class Timeout(Exception):
    pass
