import time
from taskue import Task, WorkflowStatus, TaskStatus, InvalidAction, NotFound, Timeout, logger
from taskue.runner import TaskueRunner
from tests import BaseTestCase
import random
from parameterized import parameterized


class TaskTests(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test01_enqueue_task(self):
        logger.info("enqueue task")
        task = Task()
        task.title = "mytask"
        task.execute(time.sleep, 2)
        now = time.time()
        task_uid = self.cl.enqueue(task)

        logger.info("get task and make sure its status is pending or running")
        result = self.cl.task_get(task_uid)
        self.assertIn(result.status, [TaskStatus.PENDING, TaskStatus.RUNNING])

        logger.info("wait until the task finish and verify result")
        result = self.cl.task_wait(task_uid)
        self.assertEqual(type(result.json), dict)
        self.assertEqual(result.title, "mytask")
        self.assertTrue(result.is_done)
        self.assertTrue(result.is_passed)
        self.assertFalse(result.is_failed)
        self.assertEqual(result.status, TaskStatus.PASSED)
        self.assertEqual(task.title, result.title)
        self.assertEqual(result.workflow, None)
        self.assertAlmostEqual(now, result.created_at, delta=0.1)
        self.assertAlmostEqual(now, result.started_at, delta=0.1)
        self.assertAlmostEqual(result.executed_at - result.queued_at, 2, delta=0.1)

    def test03_list_tasks(self):
        for i in range(10):
            task = Task(title=f"task {i}")
            task.execute(int, 1.5)
            self.cl.enqueue(task)

        tasks = list(self.cl.task_list(limit=10))
        self.assertEqual(len(tasks), 10)

        tasks = list(self.cl.task_list(page=1, limit=1))
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].title, "task 9")

        tasks = list(self.cl.task_list(page=10, limit=1))
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].title, "task 0")

    def test05_delete_task(self):
        task = Task()
        task.execute(time.sleep, 3)
        task_uid = self.cl.enqueue(task)

        with self.assertRaises(InvalidAction):
            self.cl.task_delete(task_uid)

        self.cl.task_wait(task_uid)
        self.cl.task_delete(task_uid)

        with self.assertRaises(NotFound):
            self.cl.task_get(task_uid)

    @parameterized.expand([("smaller", 2), ("greater", 4)])
    def test04_wait_for_task(self, name, timeout):
        task = Task()
        task.execute(time.sleep, 3)
        task_uid = self.cl.enqueue(task)

        if timeout < 3:
            with self.assertRaises(Timeout):
                self.cl.task_wait(task_uid, timeout=timeout)
        else:
            result = self.cl.task_wait(task_uid, timeout=6)
            self.assertTrue(result.is_done)

    @parameterized.expand([("smaller", 2), ("greater", 4)])
    def test06_task_timeout(self, name, timeout):
        task = Task()
        task.execute(time.sleep, 3)
        task.timeout = timeout
        task_uid = self.cl.enqueue(task)

        result = self.cl.task_wait(task_uid)
        if timeout < 3:
            self.assertEqual(result.status, TaskStatus.TIMEDOUT)
        else:
            self.assertEqual(result.status, TaskStatus.PASSED)

    @parameterized.expand([("enabled", True), ("disabled", False)])
    def test06_task_rescheduling(self, name, allow_rescheduling):
        task = Task()
        task.timeout = 5
        task.allow_rescheduling = allow_rescheduling
        task.execute(time.sleep, 4)
        task_uid = self.cl.enqueue(task)

        time.sleep(1)

        task = self.cl.task_get(task_uid)
        runner = task.runner
        self.kill_runner(runner)

        result = self.cl.task_wait(task_uid)
        if allow_rescheduling:
            self.assertEqual(result.status, TaskStatus.PASSED)
            self.assertEqual(result.rescheduleded, 1)
            self.assertNotEqual(result.runner, runner)
        else:
            self.assertEqual(result.status, TaskStatus.TERMINATED)
            self.assertEqual(result.rescheduleded, 0)
            self.assertNotEqual(result.terminated_at, None)
            self.assertEqual(result.runner, runner)

    def test07_retry_task(self):
        task = Task()
        task.retries = 3
        task.execute(time.sleep, "a")
        task_uid = self.cl.enqueue(task)

        result = self.cl.task_wait(task_uid)
        self.assertEqual(result.status, TaskStatus.FAILED)
        self.assertEqual(result.attempts, task.retries)

    def test_task_tag(self):
        task = Task()
        task.tag = random.choice(list(self.runners.keys()))
        task.execute(print, "test")

        task_uid = self.cl.enqueue(task)
        result = self.cl.task_wait(task_uid)
        self.assertEqual(result.runner, task.tag)

    def test_task_result(self):
        task = Task()
        task.execute(sum, [5, 6])
        task_uid = self.cl.enqueue(task)
        result = self.cl.task_wait(task_uid)
        self.assertEqual(result.result, 11)

