from gevent import monkey

monkey.patch_all()
import sys
import time
from uuid import uuid4
import pickle
import traceback
from signal import SIGINT, SIGKILL, SIGTERM

import gevent
import redis
import gc
import objgraph
from enum import Enum
from taskue.task import _Task, TaskStatus
from taskue.utils import RedisController, logger


HEARTBEAT_TIMEOUT = 10
HEARTBEAT_MAX_DELAY = 5


class RunnerStatus(Enum):
    IDEL = "idel"
    BUSY = "busy"
    STOPPED = "stopped"
    STOPPING = "stopping"
    DEAD = "dead"


class TaskueRunner:
    def __init__(
        self,
        redis_conn: redis.Redis,
        name: str = None,
        namespace: str = "default",
        queues: list = None,
        timeout: int = 3600,
        run_untaged_tasks: bool = True,
    ):
        self.name = name or uuid4().hex[:10]
        self.namespace = namespace
        self.queues = queues or []
        self.timeout = timeout
        self.run_untaged_tasks = run_untaged_tasks
        self.logger = logger.bind(app="RUNNER %s" % self.name)
        self._stop_flag = False
        self._queues = []
        self._rctrl = RedisController(redis_conn, namespace)
        self._rlock = self._rctrl.lock("runner")

    def _execute_task(self, task):
        func, args, kwargs = task.workload
        timeout = task.timeout or self.timeout
        for attempt in range(max(task.retries, 1)):
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

    def _start(self):
        self.logger.info("Taskue runner (ID: {}) is running", self.name)
        start = time.time()
        while not self._stop_flag:

            queue, uid = self._rctrl.blpop(self._queues, timeout=3)
            if not (queue and uid):
                print("**********************", time.time() - start)
                continue

            if queue == self._rctrl.new_workfows_queue:
                self.logger.info("Starting workflow {}", uid)
                workflow = self._rctrl.get_workflow(uid)
                workflow.start()
            else:
                task = self._rctrl.get_task(uid)
                task.runner = self.name
                task.status = TaskStatus.RUNNING
                task.started_at = time.time()
                task.save()
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

                pipeline = self._rctrl.pipeline()
                task.rctrl = self._rctrl
                task.save(pipeline=pipeline)

                if task.workflow:
                    # with self._rctrl.lock(task.workflow):
                    workflow = self._rctrl.get_workflow(task.workflow)
                    workflow.update_task(task)
                    workflow.update(pipeline=pipeline)
                    pipeline.execute()
                else:
                    pipeline.execute()

    def _wait_for_connection(self):
        while not self._stop_flag:
            try:
                if self._rctrl.ping():
                    self.logger.success("Redis connection is established, back to work")
                    break
            except redis.exceptions.ConnectionError:
                gevent.sleep(5)

    def _stop(self):
        if not self._stop_flag:
            self.logger.info("Shutting down, please wait ...")
            self._stop_flag = True
            self._rctrl.update_runner(self.name, status=RunnerStatus.STOPPING)

    def start(self):
        """ Start the runner """
        for signal_type in (SIGTERM, SIGKILL, SIGINT):
            gevent.signal(signal_type, self._stop)

        if self.run_untaged_tasks:
            self.queues.insert(0, "default")

        self._queues = [self._rctrl.new_workfows_queue, self._rctrl.events_queue]
        for queue in self.queues:
            self._queues.append(self._rctrl.queued_tasks_queue % queue)

        # self._register()
        self._start()

    def stop(self):
        """ Stop the runner gracefully """
        self._stop()


if __name__ == "__main__":
    from redis import Redis

    runner = TaskueRunner(Redis())
    runner.start()


# class TaskueRunner:
#     def __init__(
#         self,
#         redis_conn: redis.Redis,
#         name: str = None,
#         namespace: str = "default",
#         queues: tuple = None,
#         timeout: int = 3600,
#         run_untaged_tasks: bool = True,
#     ):
#         self.name = name or uuid4().hex[:10]
#         self.namespace = namespace
#         self.queues = queues or ()
#         self.timeout = timeout
#         self.run_untaged_tasks = run_untaged_tasks
#         self.logger = logger.bind(app="RUNNER %s" % self.name)
#         self._stop_flag = False
#         self._queues = []
#         self._rctrl = RedisController(redis_conn, namespace)

#     def __dir__(self):
#         return ('start', 'stop')

#     @property
#     def rstatus(self):
#         return self._rctrl.get_runner_status(self.name)

# )

#     def _wait_for_connection(self):
#         while not self._stop_flag:
#             try:
#                 if self._rctrl.ping():
#                     self.logger.success("Redis connection is established, back to work")
#                     break
#             except redis.exceptions.ConnectionError:
#                 gevent.sleep(5)

#     def _stop(self):
#         if not self._stop_flag:
#             self.logger.info("Shutting down, please wait ...")
#             self._stop_flag = True
#             self._rctrl.update_runner(self.name, status=RunnerStatus.STOPPING)

#     def start(self):
#         """ Start the runner """
#         for signal_type in (SIGTERM, SIGKILL, SIGINT):
#             gevent.signal(signal_type, self._stop)

#         self._queues = [self._rctrl.queued_tasks_queue % queues for queues in self.queues]
#         if self.run_untaged_tasks:
#             self._queues.append(self._rctrl.queued_tasks_queue % "default")

#         self._register()
#         self._start()

#     def stop(self):
#         """ Stop the runner gracefully """
#         self._stop()


# if __name__ == "__main__":
#     from redis import Redis

#     runner = TaskueRunner(Redis())
#     runner.start()
