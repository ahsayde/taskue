from gevent import monkey

monkey.patch_all()
import os
import sys
import uuid
import time
import gevent
import signal
import pickle
import traceback
from redis import Redis
from redis import exceptions as redis_exceptions
from taskue.utils import DEFAULT_QUEUE, RedisKeys, logging
from taskue.task import Task, TaskStatus, TaskLogger
from taskue.workflow import Workflow, WorkflowStatus, WorkflowStageStatus

tasklogger = TaskLogger()
logger = logging.getLogger()
logger.addHandler(tasklogger.handler)


class TaskueRunner:
    def __init__(
        self,
        redis: Redis,
        tags: list = [],
        timeout: int = 3600,
        run_untaged_tasks: bool = True,
        path: str = None,
    ):
        self.uid = uuid.uuid4().hex
        self.tags = tags
        self.timeout = timeout
        self.run_untaged_tasks = run_untaged_tasks
        self._redis = redis
        self._exit = False
        if path:
            sys.path.append(path)

    def _save(self, obj):
        self._redis.set(obj.rkey, obj.dump())
        if isinstance(obj, Task) and obj.workflow_uid:
            self._cache_status(obj)

    def _cache_status(self, task):
        self._redis.hmset(
            RedisKeys.CACHE.format(task.workflow_uid), {task.uid: task.status}
        )

    def _expire_cache(self, workflow_uid):
        self._redis.expire(RedisKeys.CACHE.format(workflow_uid), 5)

    def _queue(self, uid):
        logging.info("Schduling task %s", uid)
        task = self.get_task(uid)
        task.status = TaskStatus.QUEUED
        task.queued_at = time.time()
        self._save(task)
        self._redis.rpush(task.rqueue, task.uid)

    def get_task(self, uid):
        blob = self._redis.get(RedisKeys.TASK.format(uid))
        return pickle.loads(blob)

    def get_workflow(self, uid):
        blob = self._redis.get(RedisKeys.WORKFLOW.format(uid))
        return pickle.loads(blob)

    @property
    def rqueues(self):
        return set([RedisKeys.QUEUE.format(tag) for tag in self.tags])

    def _wait_for_connection(self):
        while not self._exit:
            try:
                if self._redis.ping():
                    logging.info("Redis connection is back")
                    break
            except redis_exceptions.ConnectionError:
                time.sleep(1)

    def _wait_for_work(self):
        queues = [RedisKeys.WORKFLOWS]
        for queue in self.rqueues:
            queues.append(queue)

        if self.run_untaged_tasks or not self.tags:
            queues.append(RedisKeys.QUEUE.format(DEFAULT_QUEUE))

        while not self._exit:
            try:
                response = self._redis.blpop(queues, timeout=3)
                if not response:
                    continue

                queue, uid = response
                if queue.decode() == RedisKeys.WORKFLOWS:
                    workflow = self.get_workflow(uid.decode())
                    workflow.started_at = time.time()
                    workflow.status = WorkflowStatus.RUNNING
                    self._save(workflow)

                    for uid in workflow.current_stage_tasks:
                        self._queue(uid)
                else:
                    task = self.get_task(uid.decode())
                    task.runner_uid = self.uid
                    task.status = TaskStatus.RUNNING
                    task.started_at = time.time()
                    self._save(task)
                    try:
                        logging.info("Executing task %s", task.uid)
                        func, args, kwargs = task.workload
                        timeout = task.timeout or self.timeout
                        for attempt in range(1, task.retries + 1):
                            logging.info(
                                "Starting attempt (%s of %s)", attempt, task.retries
                            )
                            try:
                                with gevent.Timeout(timeout):
                                    with tasklogger as tlg:
                                        task.result = func(*args, **kwargs)
                                        task.logs = tlg.get_logs()
                            except:
                                if attempt < task.retries - 1:
                                    continue
                                    
                                raise
                            else:
                                task.status = TaskStatus.PASSED
                                break

                    except gevent.Timeout:
                        task.status = TaskStatus.TIMEOUT

                    except Exception:
                        task.status = TaskStatus.FAILED
                        task.error = traceback.format_exc()
                        
                    task.executed_at = time.time()
                    self._save(task)

                    logging.info("Task is finished with status %s", task.status)
                    
                    if task.workflow_uid:
                        workflow = self.get_workflow(task.workflow_uid)
                        workflow.update_status(self._redis)
                        if workflow.is_current_stage_done:
                            if workflow.is_last_stage:
                                self._expire_cache(workflow.uid)
                            else:
                                workflow.current_stage += 1
                                logging.info(
                                    "Starting stage %s of workflow %s",
                                    workflow.current_stage,
                                    workflow.uid,
                                )
                                for uid in workflow.current_stage_tasks:
                                    self._queue(uid)

                        self._save(workflow)

            except redis_exceptions.ConnectionError:
                logging.critical("Can't connect to Redis, waiting for the connection ...")
                self._wait_for_connection()
 
    def start(self):
        logging.info("Taskue runner (ID: %s) is running", self.uid)
        signal.signal(signal.SIGINT, self.exit_handler)  
        self._wait_for_work()

    def exit_handler(self, signal, frame):
        if not self._exit:
            logging.info("Shutting down, please wait ...")
            self._exit = True


if __name__ == "__main__":
    r = TaskueRunner(Redis())
    r.start()
