import sys
import time
import redis
import pickle
from loguru import logger


class Rediskey:
    NAMESPACES = "taskue:namespaces"
    RUNNER = "taskue:{ns}:runner:{name}"
    HEARTBEAT = "taskue:{ns}:heartbeat:{name}"
    WORKFLOW = "taskue:{ns}:workflow:{uid}"
    WORKFLOWS = "taskue:{ns}:workflows"
    TASK = "taskue:{ns}:task:{uid}"


class Queue:
    NEW_WORKFLOWS = "taskue:{ns}:new:workflow"
    NEW_TASKS = "taskue:{ns}:new:tasks"
    QUEUED_TASKS = "taskue:{ns}:queued:tasks:%s"
    EVENTS = "taskue:{ns}:events"



class RedisController:
    def __init__(self, connection, namespace):
        self._connection = connection
        self._namespace = namespace
        self.new_workfows_queue = Queue.NEW_WORKFLOWS.format(ns=self.namespace)
        self.new_tasks_queue = Queue.NEW_TASKS.format(ns=self.namespace)
        self.queued_tasks_queue = Queue.QUEUED_TASKS.format(ns=self.namespace)
        self.events_queue = Queue.EVENTS.format(ns=self.namespace)

    @property
    def namespace(self):
        return self._namespace

    def ping(self):
        return self._connection.ping()

    def pipeline(self):
        return self._connection.pipeline()

    def decode_bytes(self, ddict):
        result = dict()
        for key, value in ddict.items():
            result[key.decode()] = value.decode()
        return result

    def blpop(self, queues, timeout=None):
        queue = data = None
        response = self._connection.blpop(queues, timeout=timeout)

        if response:
            queue = response[0].decode()
            data = response[1].decode()

        return queue, data

        # def namespace_list(self):
    
    def list_namespaces(self):
        namespaces = self._connection.hscan_iter(Rediskey.NAMESPACES)
        for item in namespaces:
            namespace, timestamp = item
            yield dict(name=namespace.decode(), timestamp=int(timestamp.decode()))

    def namespace_delete(self, namespace):
        pipeline = self._connection.pipeline()
        pipeline.hdel(Rediskey.NAMESPACES, namespace)
        keys = self._connection.keys("taskue:{ns}:*".format(ns=namespace))
        if keys:
            pipeline.delete(*keys)
        pipeline.execute()

    def get_runners(self):
        keys = self._connection.keys(Rediskey.RUNNER.format(ns=self.namespace, name="*"))
        for key in keys:
            yield self.decode_bytes(self._connection.hgetall(key))

    def get_runner(self, name):
        runner = self._connection.hgetall(Rediskey.RUNNER.format(ns=self.namespace, name=name))
        if runner:
            return self.decode_bytes(runner)

    def register_runner(self, name, status, queues, timeout, run_untaged_tasks):
        self._connection.hmset(
            Rediskey.RUNNER.format(ns=self.namespace, name=name),
            {
                "name": name,
                "status": status,
                "timeout": timeout,
                "queues": ",".join(queues),
                "run_untaged_tasks": int(run_untaged_tasks),
            },
        )

    def is_healthy_runner(self, name):
        return self._connection.exists(Rediskey.HEARTBEAT.format(ns=self.namespace, name=name))

    def get_runner_status(self, name):
        return self._connection.hget(
            Rediskey.RUNNER.format(ns=self.namespace, name=name), "status",
        ).decode()

    def update_runner(self, name, pipeline=None, **kwargs):
        connection = pipeline if pipeline is not None else self._connection
        return connection.hmset(
            Rediskey.RUNNER.format(ns=self.namespace, name=name), kwargs,
        )

    def save_runner(self, name, runner_dict, pipeline=None):
        connection = pipeline if pipeline is not None else self._connection
        connection.hmset(
            Rediskey.RUNNER.format(ns=self.namespace, name=name), runner_dict,
        )

    def send_runner_heartbeat(self, name, timeout, pipeline=None):
        connection = pipeline if pipeline is not None else self._connection
        connection.set(
            Rediskey.HEARTBEAT.format(ns=self.namespace, name=name), "", ex=timeout,
        )

    def get_workflow(self, uid):
        blob = self._connection.get(Rediskey.WORKFLOW.format(ns=self.namespace, uid=uid))
        if blob:
            return pickle.loads(blob)

    def save_workflow(self, workflow, queue=False, pipeline=None):
        connection = pipeline if pipeline is not None else self._connection
        connection.set(
            Rediskey.WORKFLOW.format(ns=self.namespace, uid=workflow.uid), pickle.dumps(workflow),
        )
        if queue:
            connection.zadd(
                Rediskey.WORKFLOWS.format(ns=self.namespace), workflow.uid, workflow.created_at,
            )
            connection.rpush(self.new_workfows_queue, workflow.uid)

    def delete_workflow(self, uid, pipeline=None):
        connection = pipeline if pipeline is not None else self._connection
        connection.delete(Rediskey.WORKFLOW.format(ns=self.namespace, uid=uid))
        connection.zrem(Rediskey.WORKFLOWS.format(ns=self.namespace), uid)
        # delete tasks
        tasks_keys_pattern = Rediskey.TASK.format(ns=self.namespace, uid="%s_*" % uid)
        tasks_keys = connection.keys(tasks_keys_pattern)
        connection.delete(*tasks_keys)

    def list_workflows(self, start, end, pipeline=None):
        connection = pipeline if pipeline is not None else self._connection
        return connection.zrange(Rediskey.WORKFLOWS.format(ns=self.namespace), start, end)

    def get_task(self, uid):
        blob = self._connection.get(Rediskey.TASK.format(ns=self.namespace, uid=uid))
        if blob:
            return pickle.loads(blob)

    def save_task(self, task, notify=False, queue=False, pipeline=None):
        connection = pipeline if pipeline is not None else self._connection
        connection.set(
            Rediskey.TASK.format(ns=self.namespace, uid=task.uid), pickle.dumps(task),
        )
        if notify:
            connection.rpush(self.events_queue, task.uid)

        if queue:
            connection.rpush(self.queued_tasks_queue % (task.tag or "default"), task.uid)

    def delete_task(self, uid, pipeline=None):
        connection = pipeline if pipeline is not None else self._connection
        connection.delete(Rediskey.TASK.format(ns=self.namespace, uid=uid))


logging_format = "<light-blue>{time: YYYY-MM-DD at HH:mm:ss}</> | {extra[app]} | <level>{level}</> | <level>{message}</>"
logger.configure(
    handlers=[dict(sink=sys.stderr, format=logging_format, colorize=True),]
)
