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
from taskue.task import _Task, TaskStatus
from taskue.utils import RedisController, logger


HEARTBEAT_TIMEOUT = 10
HEARTBEAT_MAX_DELAY = 5


class RunnerStatus:
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
        self._stop = False
        self._queues = []
        self._rctrl = RedisController(redis_conn, namespace)

    def _register(self):
        """Register runner
        """
        self._rctrl.register_runner(
            name=self.name,
            namespace=self.namespace,
            status=RunnerStatus.IDEL,
            queues=self.queues,
            timeout=self.timeout,
            run_untaged_tasks=self.run_untaged_tasks,
        )

    def _monitor_runners(self):
        """Monitor other runners
        """
        if not self._rctrl.acquire_monitoring_task(self.name, timeout=30):
            return
        
        self.logger.info("Start monitor runners")
        for runner in self._rctrl.get_runners():
            if (
                self._rctrl.is_healthy_runner(runner["name"])
                or runner["name"] == self.name
                or runner["status"] in [RunnerStatus.DEAD, RunnerStatus.STOPPED,]
            ):
                continue

            with self._rctrl.pipeline() as pipeline:
                self.logger.warning("Runner {} is dead", runner["name"])
                self._rctrl.update_runner(runner["name"], pipeline=pipeline, status=RunnerStatus.DEAD)
                if runner["status"] == RunnerStatus.BUSY:
                    task = self._rctrl.get_task(runner["task"])
                    if task.enable_rescheduling:
                        logger.info("Re-scheduling task {}", runner["task"])
                        task.reschedule(pipeline=pipeline)
                    else:
                        logger.info("Terminate task {}", runner["task"])
                        task.terminate(pipeline=pipeline)

                    if task.workflow:
                        workflow = self._rctrl.get_workflow(task.workflow)
                        workflow.update_task(task)
                        workflow.update(pipeline=pipeline)

                pipeline.execute()

    def _send_heartbeat(self, timeout=None):
        timeout = (timeout or HEARTBEAT_TIMEOUT) + HEARTBEAT_MAX_DELAY
        self._rctrl.send_runner_heartbeat(self.name, timeout)

    def _update(self, pipeline=None, **kwargs):
        self._rctrl.update_runner(self.name, pipeline, **kwargs)

    def _execute_task(self, task):
        func, args, kwargs = task.workload
        timeout = task.timeout or self.timeout
        for attempt in range(max(task.retries, 1)):
            task.attempts += 1
            self.logger.info("Executing attempt %s of {}", task.attempts, task.retries)

            self._send_heartbeat(timeout=timeout)
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
        while True:
            if self._stop:
                self._update(status=RunnerStatus.STOPPED)
                break

            if self._rctrl.get_runner_status(self.name) == RunnerStatus.DEAD:
                self.logger.critical("Marked as dead, shutting down")
                sys.exit(1)

            self._send_heartbeat()
            self._monitor_runners()

            queue, uid = self._rctrl.blpop(self._queues, timeout=3)
            if not (queue and uid):
                continue

            if queue == self._rctrl.new_workfows_queue:
                workflow = self._rctrl.get_workflow(uid)
                self.logger.info("start workflow {}", workflow.title)
                workflow.start()
            else:
                task = self._rctrl.get_task(uid)
                task.runner = self.name
                task.status = TaskStatus.RUNNING
                task.started_at = time.time()

                with self._rctrl.pipeline() as pipeline:
                    task.save(pipeline=pipeline)
                    self._update(pipeline=pipeline, status=RunnerStatus.BUSY, task=task.uid)

                self.logger.info("executing task {}", task.title)
                try:
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

                if self._rctrl.get_runner_status(self.name) == RunnerStatus.DEAD:
                    self.logger.critical("Marked as dead, shutting down")
                    sys.exit(1)

                with self._rctrl.pipeline() as pipeline:
                    task.save(pipeline=pipeline)
                    if task.workflow:
                        with self._rctrl.lock(task.workflow):
                            workflow = self._rctrl.get_workflow(task.workflow)
                            workflow.update_task(task)
                            workflow.update(pipeline=pipeline)

                    self._update(pipeline, status=RunnerStatus.IDEL, task=0)
                    pipeline.execute()


    def start(self):
        """ Start the runner """
        for signal_type in (SIGTERM, SIGKILL, SIGINT):
            gevent.signal(signal_type, self.stop)

        if self.run_untaged_tasks:
            self.queues.insert(0, "default")

        for queue in self.queues:
            self._queues.append(self._rctrl.queued_tasks_queue % queue)

        self._queues.append(self._rctrl.new_workfows_queue)

        self._send_heartbeat()
        self._register()

        self.logger.info("Taskue runner (name: {}) is running", self.name)
        self._start()

    def stop(self):
        """ Stop the runner gracefully """
        if not self._stop:
            self.logger.info("Shutting down gracefully, please wait ...")
            self._update(status=RunnerStatus.STOPPING)
            self._stop = True

if __name__ == "__main__":
    from redis import Redis

    runner = TaskueRunner(Redis())
    runner.start()
