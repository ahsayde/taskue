from gevent import monkey

monkey.patch_all()
import sys
import time
import uuid
import pickle
import traceback
from signal import SIGINT, SIGKILL, SIGTERM

import gevent
import redis

from taskue import logger
from taskue import _Task, TaskStatus
from taskue.utils import Queue, Rediskey


HEARTBEAT_TIMEOUT = 10
HEARTBEAT_MAX_DELAY = 5


class RunnerStatus:
    IDEL = "idel"
    BUSY = "busy"
    STOPPED = "stopped"
    STOPPING = "stopping"
    DEAD = "dead"
    ACTIVE = [IDEL, BUSY]


# def error_handler(ttype, tvalue, tb):
#     if ttype == redis.exceptions.ConnectionError:
#         wait_for_connection()

class TaskueRunner:
    def __init__(
        self,
        redis_conn: redis.Redis,
        name: str = None,
        tags: tuple = None,
        timeout: int = 3600,
        run_untaged_tasks: bool = True,
    ):
        self.uid = uuid.uuid4().hex[:5]
        self.name = name or f"RUNNER-{self.uid}"
        self.tags = tags or ()
        self.timeout = timeout
        self.current_task = 0
        self.status = RunnerStatus.IDEL
        self.run_untaged_tasks = run_untaged_tasks
        self._queues = []
        self._redis_conn = redis_conn
        self._heartbeat_thread = None
        self._stop_flag = False
        self.logger = logger.bind(app="RUNNER %s" % self.uid)

    @property
    def redis_key(self):
        return Rediskey.RUNNER % self.uid

    @property
    def rstatus(self):
        return self._redis_conn.hget(Rediskey.RUNNER % self.uid, "status").decode()

    def _register(self):
        self._redis_conn.hmset(
            self.redis_key,
            dict(
                uid=self.uid,
                name=self.name,
                task=self.current_task,
                status=RunnerStatus.IDEL,
                timeout=self.timeout,
                tags=",".join(self.tags),
                run_untaged_tasks=int(self.run_untaged_tasks),
            ),
        )

    def _load_task(self, uid):
        blob = self._redis_conn.get(Rediskey.TASK % uid)
        return pickle.loads(blob)

    def _save(self, pipeline=None):
        redis_conn = pipeline or self._redis_conn
        redis_conn.hmset(self.redis_key, dict(status=self.status, task=self.current_task))

    def _save_progress(self, task):
        pipeline = self._redis_conn.pipeline()
        task.save(pipeline, notify=True)
        self._save(pipeline)
        pipeline.execute()

    def _send_heartbeat(self, timeout=None):
        timeout = (timeout or HEARTBEAT_TIMEOUT) + HEARTBEAT_MAX_DELAY
        self.logger.info("Sending heartbeat with timeout: {}", timeout)
        self._redis_conn.set(Rediskey.HEARTBEAT % self.uid, "", ex=timeout)

    def _execute_task(self, task):
        func, args, kwargs = task.workload
        timeout = task.timeout or self.timeout
        for attempt in range(max(task.retries, 1)):
            self._send_heartbeat(timeout=timeout)

            task.attempts += 1
            self.logger.info("Executing attempt %s of {}", task.attempts, task.retries)
            try:
                if timeout:
                    task.result = gevent.with_timeout(timeout, func, *args, **kwargs)
                else:
                    task.result = func(*args, **kwargs)
            except Exception as err:
                if attempt < task.retries - 1:
                    self.logger.exception(str(err))
                    continue
                else:
                    raise err
            else:
                task.status = TaskStatus.PASSED
                break

    def _check_status(self):
        if self.rstatus == RunnerStatus.DEAD:
            self.logger.critical("Runner is marked as DEAD, Shutting down")
            sys.exit(1)

    def _start(self):
        self.logger.success("Taskue runner (ID: {}) is running", self.uid) 
        while True:
            if self._stop_flag:
                self.logger.success("All is done, Bye bye!")
                self.status = RunnerStatus.STOPPED
                self._save()
                break

            self._check_status()
            self._send_heartbeat()

            response = self._redis_conn.blpop(self._queues, timeout=HEARTBEAT_TIMEOUT)
            if not response:
                continue

            task = self._load_task(response[1].decode())
            task.runner = self.uid
            task.status = TaskStatus.RUNNING
            task.started_at = time.time()

            self.current_task = task.uid
            self.status = RunnerStatus.BUSY
            self._save_progress(task)

            try:
                self.logger.info("Picked task {} up", task.uid)
                self._execute_task(task)

            except gevent.Timeout:
                task.status = TaskStatus.TIMEDOUT

            except pickle.UnpicklingError as e:
                task.status = TaskStatus.ERRORED
                task.result = str(e)

            except:
                task.status = TaskStatus.FAILED
                task.result = traceback.format_exc()

            finally:
                task.executed_at = time.time()


            self._check_status()

            self.current_task = 0
            self.status = RunnerStatus.IDEL
            self._save_progress(task)

            self.logger.info("Task {} finished with status {}", task.uid, task.status.value)
 

    def _wait_for_connection(self):
        while not self._stop_flag:
            try:
                if self._redis_conn.ping():
                    self.logger.success("Redis connection is established, back to work")
                    break
            except redis.exceptions.ConnectionError:
                gevent.sleep(5)

    def _stop(self):
        if not self._stop_flag:
            self.logger.info("Shutting down, please wait ...")
            self._stop_flag = True
            self.status = RunnerStatus.STOPPING
            self._save()

    def start(self):
        """ Start the runner """
        for signal_type in (SIGTERM, SIGKILL, SIGINT):
            gevent.signal(signal_type, self._stop)

        self._queues = [Queue.CUSTOM % tag for tag in self.tags]
        if self.run_untaged_tasks:
            self._queues.append(Queue.DEFAULT)

        self._register()
        self._start()

    def stop(self):
        """ Stop the runner gracefully """
        self._stop()


if __name__ == "__main__":
    runner = TaskueRunner(redis.Redis())
    runner.start()
