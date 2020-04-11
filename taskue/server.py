from gevent import monkey

monkey.patch_all()

import time
import gevent
import redis
import pickle
from signal import SIGINT, SIGKILL, SIGTERM

from taskue.runner import RunnerStatus
from taskue.task import _Task
from taskue.workflow import _Workflow
from taskue.utils import Queue, Rediskey, _decode, logger


BLOCK_TIMEOUT = 5
HEARTBEAT_TIMEOUT = 60
logger = logger.bind(app="SERVER")


class TaskueServer:
    def __init__(self, redis_conn: redis.Redis):
        self._redis_conn = redis_conn
        self._stop_flag = False

    @property
    def _runners(self):
        for key in self._redis_conn.keys(Rediskey.RUNNER % "*"):
            yield _decode(self._redis_conn.hgetall(key))

    def _load_task(self, uid):
        blob = self._redis_conn.get(Rediskey.TASK % uid)
        return pickle.loads(blob)

    def _load_workflow(self, uid):
        blob = self._redis_conn.get(Rediskey.WORKFLOW % uid)
        workflow = pickle.loads(blob)
        workflow._redis_conn = self._redis_conn
        return workflow

    def _is_healthy_runner(self, uid):
        return self._redis_conn.exists(Rediskey.HEARTBEAT % uid)

    def _disable_runner(self, runner_uid, pipeline):
        pipeline.hset("taskue:runner:%s" % runner_uid, "status", RunnerStatus.DEAD)

    def _monitor_runners(self):
        while not self._stop_flag:
            for runner in self._runners:
                if (
                    self._is_healthy_runner(runner["uid"])
                    or runner["status"] not in RunnerStatus.ACTIVE
                ):
                    continue

                logger.warning("Runner {} is dead", runner["uid"])
                pipeline = self._redis_conn.pipeline()
                self._disable_runner(runner["uid"], pipeline)

                if runner["status"] == RunnerStatus.BUSY:
                    task = self._load_task(runner["task"])
                    workflow = self._load_workflow(task.workflow)

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
                if self._redis_conn.ping():
                    logger.info("Redis connection is established, back to work")
                    break
            except redis.exceptions.ConnectionError:
                gevent.sleep(5)

    def _start(self):
        while not self._stop_flag:
            try:
                response = self._redis_conn.blpop(
                    (Queue.EVENTS, Queue.WORKFLOWS), timeout=BLOCK_TIMEOUT
                )
                if not response:
                    continue

                queue, uid = response[0].decode(), response[1].decode()
                if queue == Queue.WORKFLOWS:
                    logger.info("Starting workflow {}", uid)
                    workflow = self._load_workflow(uid)
                    workflow.start()

                elif queue == Queue.EVENTS:
                    task = self._load_task(uid)
                    workflow = self._load_workflow(task.workflow)

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
    server = TaskueServer(redis.Redis())
    server.start()
