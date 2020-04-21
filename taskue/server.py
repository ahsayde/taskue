from gevent import monkey

monkey.patch_all()

import redis
import gevent
from signal import SIGINT, SIGKILL, SIGTERM
from taskue.runner import RunnerStatus
from taskue.utils import logger, RedisController


BLOCK_TIMEOUT = 5
HEARTBEAT_TIMEOUT = 60
logger = logger.bind(app="SERVER")


class TaskueServer:
    def __init__(self, redis_conn: redis.Redis, namespace: str = "default"):
        self.namespace = namespace
        self._stop_flag = False
        self._rctrl = RedisController(redis_conn, namespace)

    def __dir__(self):
        return ("namespace", "start", "stop")

    def _disable_runner(self, runner_name, pipeline):
        self._rctrl.update_runner(runner_name, status=RunnerStatus.DEAD, task=0, pipeline=pipeline)

    def _monitor_runners(self):
        while not self._stop_flag:
            for runner in self._rctrl.get_runners():
                if (
                    self._rctrl.is_healthy_runner(runner["name"])
                    or runner["status"] not in RunnerStatus.ACTIVE
                ):
                    continue

                logger.warning("Runner {} is dead", runner["name"])
                pipeline = self._rctrl.pipeline()
                self._disable_runner(runner["name"], pipeline)

                if runner["status"] == RunnerStatus.BUSY:
                    task = self._rctrl.get_task(runner["task"])
                    workflow = self._rctrl.get_workflow(task.workflow)

                    if task.enable_rescheduling:
                        logger.info("Re-scheduling task {}", runner["task"])
                        workflow.reschedule_task(task, pipeline)
                    else:
                        logger.info("Terminate task {}", runner["task"])
                        workflow.terminate_task(task, pipeline)

                    workflow.update_task(task)
                    workflow.update(pipeline)

                pipeline.execute()

            gevent.sleep(HEARTBEAT_TIMEOUT)

    def _wait_for_connection(self):
        while not self._stop_flag:
            try:
                if self._rctrl.ping():
                    logger.info("Redis connection is established, back to work")
                    break
            except redis.exceptions.ConnectionError:
                gevent.sleep(5)

    def _start(self):
        while not self._stop_flag:
            try:
                queue, uid = self._rctrl.blpop(
                    (self._rctrl.workfows_queue, self._rctrl.events_queue), timeout=BLOCK_TIMEOUT
                )
                if not (queue and uid):
                    continue

                if queue == self._rctrl.workfows_queue:
                    logger.info("Starting workflow {}", uid)
                    workflow = self._rctrl.get_workflow(uid)
                    workflow.rctrl = self._rctrl
                    workflow.start()

                elif queue == self._rctrl.events_queue:
                    task = self._rctrl.get_task(uid)
                    workflow = self._rctrl.get_workflow(task.workflow)
                    workflow.rctrl = self._rctrl

                    if task.status == workflow.get_task_status(task):
                        continue

                    workflow.update_task(task)
                    workflow.update()

            except redis.exceptions.ConnectionError:
                logger.critical("Cannot connect to redis")
                self._wait_for_connection()

    def _stop(self):
        if not self._stop_flag:
            logger.info("Shutting down, please wait ...")
            self._stop_flag = True

    def start(self):
        """ Start the server """
        for signal_type in (SIGTERM, SIGKILL, SIGINT):
            gevent.signal(signal_type, self._stop)

        gevent.spawn(self._monitor_runners)
        self._start()

    def stop(self):
        """ Stop the server """
        self._stop()


if __name__ == "__main__":
    from redis import Redis

    server = TaskueServer(Redis())
    server.start()
