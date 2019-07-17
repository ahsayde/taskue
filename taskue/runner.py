from gevent import monkey

monkey.patch_all()
import sys
import uuid
import time
import gevent
import pickle
import traceback
from redis import Redis
from taskue import (
    logging,
    TaskLogger,
    RedisKeys,
    TaskStatus,
    RunnerStatus,
    DEFAULT_QUEUE,
)

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
        **kwargs
    ):
        """[summary]
        
        Arguments:
            redis {Redis} -- [description]
        
        Keyword Arguments:
            tags {list} -- runner tags (default: {[]})
            timeout {int} -- runner timeout (Note: task timeout overwrites it) (default: {3600})
            run_untaged_tasks {bool} -- flag to allow runner to run untaged tasks (default: {True})
        """
        self.uid = uuid.uuid4().hex
        self.tags = tags
        self.timeout = timeout
        self.run_untaged_tasks = run_untaged_tasks
        self.status = RunnerStatus.READY
        self._redis = redis
        if path:
            sys.path.append(path)


    def _notify_task_update(self, task):
        self._redis.rpush(RedisKeys.TASK_EVENTS, pickle.dumps(task))

    def _save_task(self, task):
        rkey = RedisKeys.TASK.format(task.uid)
        self._redis.set(rkey, pickle.dumps(task))
        self._notify_task_update(task)

    def wait_for_work(self):
        queues = set([RedisKeys.QUEUE.format(tag) for tag in self.tags])
        if self.run_untaged_tasks or not self.tags:
            queues.add(RedisKeys.QUEUE.format(DEFAULT_QUEUE))

        while True:
            _, blob = response = self._redis.blpop(queues)
            task = pickle.loads(blob)
            task.runner_uid = self.uid
            task.status = TaskStatus.RUNNING
            task.started_at = time.time()
            self.status = RunnerStatus.BUSY

            # update task info
            self._save_task(task)

            logging.info("[*] Executing task %s", task.uid)
            try:
                for attempt in range(1, task.retries + 1):
                    logging.info("Starting attempt %s of %s", attempt, task.retries)
                    try:
                        func, args, kwargs = pickle.loads(task.workload)
                        timeout = task.timeout or self.timeout
                        if timeout:
                            with tasklogger as tlg:
                                task.result = gevent.with_timeout(
                                    timeout, func, *args, **kwargs
                                )
                                task.logs = tlg.get_logs()
                        else:
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

            except Exception as e:
                task.status = TaskStatus.FAILED
                task.result = traceback.format_exc()

            finally:
                logging.info("Task is finished with status %s", task.status)
                task.executed_at = time.time()

            task.executed_at = time.time()
            self.status = RunnerStatus.READY
            self._save_task(task)

    def start(self):
        logging.info("Taskue runner (ID: %s) is running", self.uid)
        self.wait_for_work()
