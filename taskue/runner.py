from gevent import monkey

monkey.patch_all()

import time
import uuid
import pickle
import traceback
from signal import SIGINT, SIGKILL, SIGTERM

import gevent
import redis

from taskue import logging
from taskue import Task, RunnerStatus, TaskStatus, Queue, Rediskey


logger = logging.getLogger(name="Runner")

HEARTBEAT_TIMEOUT = 10
HEARTBEAT_MAX_DELAY = 5


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
                run_untaged_tasks=int(self.run_untaged_tasks)
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
        task._save(pipeline, notify=True)
        self._save(pipeline)
        pipeline.execute()

    def _send_heartbeat(self, timeout=None):
        timeout = (timeout or HEARTBEAT_TIMEOUT) + HEARTBEAT_MAX_DELAY
        self._redis_conn.set(Rediskey.HEARTBEAT % self.uid, "", ex=timeout)

    def _start(self):
        while True:
            if self._stop_flag:
                logging.info("All is done, Bye bye!")
                self.status = RunnerStatus.STOPPED
                self._save()
                break

            if self.rstatus == RunnerStatus.DEAD:
                logging.info("Marked as DEAD runner by the server, closing ...")
                break

            logging.info("Sending heartbeat with timeout: %s", HEARTBEAT_TIMEOUT)
            self._send_heartbeat()

            response = self._redis_conn.blpop(self._queues, timeout=HEARTBEAT_TIMEOUT)
            if not response:
                continue

            task = self._load_task(response[1].decode())
            task.result.runner = self.uid
            task.result.status = TaskStatus.RUNNING
            task.result.started_at = time.time()

            self.current_task = task.uid
            self.status = RunnerStatus.BUSY
            self._save_progress(task)

            try:
                logging.info("Picked task %s up", task.uid)
                func, args, kwargs = task.workload
                timeout = task.timeout or self.timeout

                for attempt in range(max(task.retries, 1)):

                    logging.info("Sending heartbeat with timeout: %s", timeout)
                    self._send_heartbeat(timeout=timeout)

                    task.result.attempts += 1
                    logging.info("Executing attempt %s of %s", task.result.attempts, task.retries)

                    try:
                        if timeout:
                            task.result.result = gevent.with_timeout(timeout, func, *args, **kwargs)
                        else:
                            task.result.result = func(*args, **kwargs)
                    except Exception as err:
                        if attempt < task.retries - 1:
                            logging.exception(str(err))
                            continue
                        else:
                            raise err
                    else:
                        task.result.status = TaskStatus.PASSED
                        break

            except gevent.Timeout:
                task.result.status = TaskStatus.TIMEDOUT

            except pickle.UnpicklingError as e:
                task.result.status = TaskStatus.ERRORED
                task.result.result = str(e)

            except:
                task.result.status = TaskStatus.FAILED
                task.result.result = traceback.format_exc()

            finally:
                task.result.executed_at = time.time()

            self.current_task = 0
            self.status = RunnerStatus.IDEL
            self._save_progress(task)

            logging.info("Task %s finished with status %s", task.uid, task.result.status)

    def _wait_for_connection(self):
        while not self._stop_flag:
            try:
                if self._redis_conn.ping():
                    logging.info("Redis connection is established, back to work")
                    break
            except redis.exceptions.ConnectionError:
                gevent.sleep(1)

    def _stop(self):
        if not self._stop_flag:
            logging.info("Shutting down, please wait ...")
            self._stop_flag = True
            self.status = RunnerStatus.STOPPING
            self._save()

    def start(self):
        """ Start the runner """
        for signal_type in (SIGTERM, SIGKILL, SIGINT):
            gevent.signal(signal_type, self._stop)

        logging.info("Taskue runner (ID: %s) is running", self.uid)

        self._queues = [Queue.CUSTOM % tag for tag in self.tags]

        if self.run_untaged_tasks:
            self._queues.append(Queue.DEFAULT)
        
        logging.info(self._queues)

        self._register()
        self._start()

    def stop(self):
        """ Stop the runner gracefully """
        self._stop()


if __name__ == "__main__":
    runner = TaskueRunner(redis.Redis())
    runner.start()
