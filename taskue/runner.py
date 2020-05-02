import importlib
import inspect
import os
import pickle
import signal
import sys
import time
import traceback
from uuid import uuid4

import gevent
import redis
from taskue import logger
from taskue.controller import RedisController
from taskue.task import TaskStatus, _Task

HEARTBEAT_TIMEOUT = 10
HEARTBEAT_MAX_DELAY = 5


class RunnerStatus:
    IDEL = "idel"
    BUSY = "busy"
    STOPPED = "stopped"
    STOPPING = "stopping"
    DEAD = "dead"


class Loader:
    def __init__(self, task):
        self.task = task

    def __enter__(self):
        name, path = self.task.workload_info["module_name"], self.task.workload_info["module_path"]
        if name not in sys.modules:
            if os.path.exists(path):
                spec = importlib.util.spec_from_file_location(name, path)
                module = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = module
                spec.loader.exec_module(module)
            else:
                raise ModuleNotFoundError(f"module {path} is not found")

    def __exit__(self, exc_type, exc_val, exc_tb):
        del sys.modules[self.task.workload_info["module_name"]]


class TaskueRunner:
    def __init__(
        self,
        redis_conn: redis.Redis,
        name: str = None,
        namespace: str = "default",
        queues: list = None,
        timeout: int = 3600,
        auto_load_modules: bool = False,
    ):
        self.name = name or uuid4().hex[:10]
        self.namespace = namespace
        self.queues = queues or []
        self.timeout = timeout
        self.auto_load_modules = auto_load_modules
        self._stop = False
        self._queues = []
        self._ctrl = RedisController(redis_conn, namespace)

    def _register(self):
        """Register runner
        """
        self._ctrl.runner_register(
            name=self.name, namespace=self.namespace, status=RunnerStatus.IDEL, queues=self.queues, timeout=self.timeout
        )

    def _monitor_runners(self):
        """Monitor other runners and reschedule dead runners's tasks
        """
        # skip if monitoring job is done by another runner in the last 30 seconds
        if not self._ctrl.acquire_monitoring_task(self.name, timeout=30):
            return

        for runner in self._ctrl.runners_list():
            # skip current runner, healthy and none active runners
            if (
                self._ctrl.is_runner_healthy(runner["name"])
                or runner["name"] == self.name
                or runner["status"] in [RunnerStatus.DEAD, RunnerStatus.STOPPED]
            ):
                continue

            with self._ctrl.pipeline() as pipeline:
                # change runner status to dead
                logger.warning("Runner {} is dead", runner["name"])
                self._ctrl.runner_update(runner["name"], pipeline=pipeline, status=RunnerStatus.DEAD)

                # if the runner is busy then reschedule its task if rescheduling is enabled
                if runner["status"] == RunnerStatus.BUSY:
                    task = self._ctrl.task_get(runner["task"])
                    if task.allow_rescheduling:
                        logger.info("Re-scheduling task {}", runner["task"])
                        task.reschedule(self._ctrl, pipeline=pipeline)
                    else:
                        logger.info("Terminate task {}", runner["task"])
                        task.terminate(self._ctrl, pipeline=pipeline)

                    # if the task is part of a workflow, update the workflow
                    if task.workflow:
                        workflow = self._ctrl.workflow_get(task.workflow)
                        workflow.update_task(task)
                        workflow.update(self._ctrl, pipeline=pipeline)

                pipeline.execute()

    def _send_heartbeat(self, timeout=None):
        """Send heartbeat with timeout
        """
        logger.debug("Sending heartbeat")
        timeout = (timeout or HEARTBEAT_TIMEOUT) + HEARTBEAT_MAX_DELAY
        self._ctrl.heartbeat_send(self.name, timeout)

    def _update(self, pipeline=None, **kwargs):
        """update runner info
        """
        self._ctrl.runner_update(self.name, pipeline, **kwargs)

    def _die_if_marked_as_dead(self):
        if self._ctrl.runner_status_get(self.name) == RunnerStatus.DEAD:
            logger.critical("Marked as dead, shutting down")
            sys.exit(1)

    def _timeout_handler(self, signum, frame):
        raise TimeoutError()

    def _execute_task(self, task):
        func, args, kwargs = task.workload
        timeout = task.timeout or self.timeout

        for attempt in range(max(task.retries, 1)):
            # send heartbeat with timeout of the task
            self._send_heartbeat(timeout=timeout)
            # set signal alarm
            if timeout:
                signal.signal(signal.SIGALRM, self._timeout_handler)
                signal.alarm(timeout)
            try:
                task.result = func(*args, **kwargs)
                task.status = TaskStatus.PASSED
            except Exception as e:
                if attempt < task.retries - 1:
                    logger.exception(e)
                    continue
                else:
                    raise
            finally:
                task.attempts += 1
                signal.alarm(0)

    def _start(self):
        while True:
            # exit if the runner marked as dead by other runners
            self._die_if_marked_as_dead()

            # exit if recived stop signal
            if self._stop:
                self._update(status=RunnerStatus.STOPPED)
                sys.exit()

            # send heartbeat
            self._send_heartbeat()

            # monitor other runners
            self._monitor_runners()

            # wait for new workflows or tasks
            queue, uid = self._ctrl.blpop(self._queues, timeout=3)
            if not (queue and uid):
                continue

            if queue == self._ctrl.keys.new_workflows:
                logger.info("Starting workflow (UID: {})", uid)
                workflow = self._ctrl.workflow_get(uid)
                workflow.start(self._ctrl)
            else:
                logger.info("Executing task (UID: {})", uid)
                task = self._ctrl.task_get(uid)
                task.runner = self.name
                task.status = TaskStatus.RUNNING
                task.started_at = time.time()

                with self._ctrl.pipeline() as pipeline:
                    # update task status to running
                    task.save(self._ctrl, pipeline=pipeline)
                    # update runner status to busy and set its task to the task uid
                    self._update(pipeline=pipeline, status=RunnerStatus.BUSY, task=task.uid)

                try:
                    # load task module if auto loading is enabled
                    if self.auto_load_modules:
                        with Loader(task):
                            self._execute_task(task)
                    else:
                        self._execute_task(task)

                except (pickle.UnpicklingError, ModuleNotFoundError):
                    task.status = TaskStatus.ERRORED
                    task.result = traceback.format_exc()

                except TimeoutError:
                    task.status = TaskStatus.TIMEDOUT

                except Exception:
                    task.status = TaskStatus.FAILED
                    task.result = traceback.format_exc()

                finally:
                    task.executed_at = time.time()

                # exit without saving task if marked as dead
                self._die_if_marked_as_dead()

                with self._ctrl.pipeline() as pipeline:
                    # save task result
                    task.save(self._ctrl, pipeline=pipeline)
                    # if task is part of workflow, update workflow
                    if task.workflow:
                        # acquire lock for this workflow to prevent race conditions
                        with self._ctrl.lock(task.workflow):
                            workflow = self._ctrl.workflow_get(task.workflow)
                            workflow.update_task(task)
                            workflow.update(self._ctrl, pipeline=pipeline)
                    # set runner status to idle
                    self._update(pipeline, status=RunnerStatus.IDEL, task=0)
                    pipeline.execute()

    def start(self):
        """ Start the runner """
        # register signals handler
        for signal_type in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signal_type, self.stop)

        self._queues.append(self._ctrl.keys.task_queue % "default")
        for queue in self.queues:
            self._queues.append(self._ctrl.keys.task_queue % queue)

        # add new workflows queue
        self._queues.append(self._ctrl.keys.new_workflows)

        # send heartbeat
        self._send_heartbeat()

        # register runner to the system
        self._register()

        # start work loop
        logger.info("Taskue runner (name: {}) is started", self.name)
        self._start()

    def stop(self):
        """ Stop the runner gracefully """
        logger.info("Shutting down gracefully, please wait ...")
        self._update(status=RunnerStatus.STOPPING)
        self._stop = True


if __name__ == "__main__":
    from redis import Redis

    runner = TaskueRunner(Redis())
    runner.start()
