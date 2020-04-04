from gevent import monkey

monkey.patch_all()

import time
import gevent
import redis
import pickle
from signal import SIGINT, SIGKILL, SIGTERM

from taskue import logging, Task, _TaskResult, _Workflow, _decode, RunnerStatus, Queue, Rediskey


HEARTBEAT_TIMEOUT = 60
logger = logging.getLogger(name="Runner")


class TaskueServer:
    def __init__(self, redis_conn: redis.Redis):
        self._redis_conn = redis_conn
        self._stop_flag = False

    @property
    def runners(self):
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

    def _disable_runner(self, runner_uid, pipeline):
        pipeline.hset("taskue:runner:%s" % runner_uid, "status", RunnerStatus.DEAD)

    def _monitor_runners(self):
        while not self._stop_flag:
            for runner in self.runners:
                if (
                    self._redis_conn.exists(Rediskey.HEARTBEAT % runner["uid"])
                    or runner["status"] not in RunnerStatus.ACTIVE
                ):
                    continue

                logging.warning("Runner %s is dead", runner["uid"])
                pipeline = self._redis_conn.pipeline()
                self._disable_runner(runner["uid"], pipeline)

                if runner["status"] == RunnerStatus.BUSY:
                    task = self._load_task(runner["task"])
                    workflow = self._load_workflow(task.workflow)

                    if task.enable_rescheduling:
                        logging.info("Re-scheduling task %s", runner["task"])
                        workflow.reschedule_task(task, pipeline)
                    else:
                        logging.info("Terminate task %s", runner["task"])
                        workflow.terminate_task(task, pipeline)

                    workflow.update_task_status(task)
                    workflow.update(pipeline)

                pipeline.execute()

            gevent.sleep(HEARTBEAT_TIMEOUT)

    def _start(self):
        while not self._stop_flag:
            
            response = self._redis_conn.blpop((Queue.EVENTS, Queue.WORKFLOWS), timeout=5)
            if not response:
                continue

            queue, uid = response[0].decode(), response[1].decode()
            if queue == Queue.WORKFLOWS:
                logging.info("Starting workflow %s", uid)
                workflow = self._load_workflow(uid)
                workflow.start()

            elif queue == Queue.EVENTS:
                task = self._load_task(uid)
                workflow = self._load_workflow(task.workflow)

                if task.result.status == workflow.get_task_status(task):
                    continue

                workflow.update_task_status(task)
                workflow.update()

    def _stop(self):
        if not self._stop_flag:
            logging.info("Shutting down, please wait ...")
            self._stop_flag = True
            self._heartbeat_greenlet.kill()

    def start(self):
        """ Start the server """
        for signal_type in (SIGTERM, SIGKILL, SIGINT):
            gevent.signal(signal_type, self._stop)

        self._heartbeat_greenlet = gevent.Greenlet(self._monitor_runners)
        self._heartbeat_greenlet.start()

        self._start()

    def stop(self):
        """ Stop the server """
        self._stop()


if __name__ == "__main__":
    server = TaskueServer(redis.Redis())
    server.start()
