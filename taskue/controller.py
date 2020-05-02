import pickle
import sys
import time

from typing import Iterator
from loguru import logger
from redis import Redis
from redis.client import Pipeline
from redis.lock import Lock
from taskue.task import _Task
from taskue.workflow import _Workflow

class Keys:
    def __init__(self, namespace: str = "default"):
        """Helper class to generate redis keys

        Keyword Arguments:
            namespace {str} -- namespace name (default: {"default"})
        """
        self.prefix = "taskue"
        self.namespace = namespace

    @property
    def namespaces(self):
        return f"{self.prefix}:{self.namespace}:namespaces"

    @property
    def runner(self):
        return f"{self.prefix}:{self.namespace}:runner:%s"

    @property
    def heartbeat(self):
        return f"{self.prefix}:{self.namespace}:heartbeat:%s"

    @property
    def monitoring_job(self):
        return f"{self.prefix}:{self.namespace}:monitoring"

    @property
    def workflow(self):
        return f"{self.prefix}:{self.namespace}:workflow:%s"

    @property
    def workflows(self):
        return f"{self.prefix}:{self.namespace}:workflows"

    @property
    def task(self):
        return f"{self.prefix}:{self.namespace}:task:%s"

    @property
    def tasks(self):
        return f"{self.prefix}:{self.namespace}:tasks"

    @property
    def new_workflows(self):
        return f"{self.prefix}:{self.namespace}:new:workflows"

    @property
    def task_queue(self):
        return f"{self.prefix}:{self.namespace}:queued:%s"

    @property
    def events(self):
        return f"{self.prefix}:{self.namespace}:events"

    def get_namespace_keys(self, namespace):
        return f"{self.prefix}:{namespace}:*"


class RedisController:
    def __init__(self, connection: Redis, namespace: str):
        """Redis controller class

        Arguments:
            connection {Redis} -- redis connection
            namespace {str} -- namespace name
        """
        self._connection = connection
        self._namespace = namespace
        self.keys = Keys(namespace=namespace)

    def decode_bytes(self, ddict: dict) -> dict:
        result = dict()
        for key, value in ddict.items():
            result[key.decode()] = value.decode()
        return result

    def ping(self) -> bool:
        return self._connection.ping()

    def pipeline(self) -> Pipeline:
        return self._connection.pipeline()

    def lock(self, name: str, sleep: float = 0.01) -> Lock:
        return self._connection.lock(name, sleep=sleep)

    def blpop(self, queues: list, timeout: int = None) -> tuple:
        queue = data = None
        response = self._connection.blpop(queues, timeout=timeout)
        if response:
            queue = response[0].decode()
            data = response[1].decode()
        return queue, data

    def namespaces_list(self) -> Iterator[dict]:
        namespaces = self._connection.hscan_iter(self.keys.namespaces)
        for item in namespaces:
            namespace, timestamp = item
            yield dict(name=namespace.decode(), timestamp=int(timestamp.decode()))

    def namespace_delete(self, namespace: str):
        pipeline = self._connection.pipeline()
        pipeline.hdel(self.keys.namespaces, namespace)
        namespace_keys = self.keys.get_namespace_keys(namespace)

        if namespace_keys:
            pipeline.delete(*namespace_keys)

        pipeline.execute()

    def runners_list(self) -> Iterator[dict]:
        keys = self._connection.keys(self.keys.runner % "*")
        for key in keys:
            yield self.decode_bytes(self._connection.hgetall(key))

    def runner_get(self, runner_name: str) -> dict:
        runner = self._connection.hgetall(self.keys.runner % runner_name)
        if runner:
            return self.decode_bytes(runner)

    def runner_register(self, name: str, namespace: str, status: str, queues: list, timeout: int):
        data = {"name": name, "namespace": namespace, "status": status, "timeout": timeout, "queues": ",".join(queues)}
        self._connection.hmset(self.keys.runner % name, data)

    def is_runner_healthy(self, name: str) -> bool:
        return self._connection.exists(self.keys.heartbeat % name)

    def acquire_monitoring_task(self, name: str, timeout: int = 30) -> bool:
        return self._connection.set(self.keys.monitoring_job, name, ex=timeout, nx=True)

    def runner_status_get(self, name: str) -> str:
        return self._connection.hget(self.keys.runner % name, "status",).decode()

    def runner_update(self, name: str, pipeline: Pipeline = None, **kwargs):
        connection = pipeline if pipeline is not None else self._connection
        return connection.hmset(self.keys.runner % name, kwargs)

    def runner_save(self, name: str, ddict: dict, pipeline: Pipeline = None):
        connection = pipeline if pipeline is not None else self._connection
        connection.hmset(self.keys.runner % name, ddict)

    def heartbeat_send(self, name: str, timeout: int, pipeline: Pipeline = None):
        connection = pipeline if pipeline is not None else self._connection
        connection.set(self.keys.heartbeat % name, "", ex=timeout)

    def workflow_get(self, workflow_uid: str, pipeline: Pipeline = None) -> _Workflow:
        connection = pipeline if pipeline is not None else self._connection
        blob = connection.get(self.keys.workflow % workflow_uid)
        if blob:
            return pickle.loads(blob)

    def workflow_save(self, workflow, queue=False, pipeline: Pipeline = None):
        connection = pipeline if pipeline is not None else self._connection
        connection.set(self.keys.workflow % workflow.uid, pickle.dumps(workflow))
        if queue:
            connection.zadd(
                self.keys.workflows, {self.keys.workflow % workflow.uid: workflow.created_at},
            )
            connection.rpush(self.keys.new_workflows, workflow.uid)

    def workflows_list(self, start: int, end: int, pipeline: Pipeline = None) -> Iterator[_Workflow]:
        connection = pipeline if pipeline is not None else self._connection
        keys = connection.zrange(self.keys.workflows, start, end, desc=True)
        if keys:
            results = connection.mget(keys)
            for blob in results:
                if blob:
                    yield pickle.loads(blob)

    def workflows_count(self) -> int:
        return self._connection.zcard(self.keys.workflows)

    def workflow_delete(self, workflow_uid: str, pipeline: Pipeline = None):
        connection = pipeline if pipeline is not None else self._connection
        connection.delete(self.keys.workflow % workflow_uid)
        connection.zrem(self.keys.workflows, workflow_uid)

    def task_get(self, task_uid: str) -> _Task:
        blob = self._connection.get(self.keys.task % task_uid)
        if blob:
            return pickle.loads(blob)

    def task_save(self, task, notify: bool = False, queue: bool = False, pipeline: Pipeline = None):
        connection = pipeline if pipeline is not None else self._connection
        connection.set(self.keys.task % task.uid, pickle.dumps(task))

        if notify:
            connection.rpush(self.keys.events, task.uid)

        if queue:
            connection.zadd(
                self.keys.tasks, {self.keys.task % task.uid: task.created_at},
            )
            connection.rpush(self.keys.task_queue % (task.tag or "default"), task.uid)

    def tasks_list(self, start: int, end: int, pipeline: Pipeline = None) -> Iterator[_Task]:
        connection = pipeline if pipeline is not None else self._connection
        keys = connection.zrange(self.keys.tasks, start, end, desc=True)
        if keys:
            results = connection.mget(keys)
            for blob in results:
                if blob:
                    yield pickle.loads(blob)

    def tasks_count(self) -> int:
        return self._connection.zcard(self.keys.tasks)

    def task_delete(self, task_uid: str, pipeline: Pipeline = None):
        connection = pipeline if pipeline is not None else self._connection
        connection.delete(self.keys.task % task_uid)
        connection.zrem(self.keys.tasks, task_uid)
