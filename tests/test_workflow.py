import time
from taskue import Task, WorkflowStatus, TaskStatus, InvalidAction, NotFound, Timeout, logger, Conditions
from taskue.runner import TaskueRunner
from tests import BaseTestCase
from parameterized import parameterized


class WorkflowTests(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test01(self):
        task = Task()
        task.execute(time.sleep, 3)
        workflow_uid = self.cl.enqueue([[task]], title="myworkflow")

        result = self.cl.workflow_get(workflow_uid)
        self.assertEqual(result.status, WorkflowStatus.PENDING)

        time.sleep(1)

        result = self.cl.workflow_get(workflow_uid)
        self.assertEqual(result.status, WorkflowStatus.RUNNING)

        result = self.cl.workflow_wait(workflow_uid)
        self.assertEqual(result.status, WorkflowStatus.PASSED)
        self.assertEqual(result.title, "myworkflow")
        self.assertTrue(result.is_done)
        self.assertTrue(result.is_passed)
        self.assertFalse(result.is_failed)

    def test03_list_workflows(self):
        for i in range(10):
            task = Task()
            task.execute(int, 1.5)
            self.cl.enqueue([[task]], title=f"workflow {i}")

        workflows = list(self.cl.workflow_list(limit=10))
        self.assertEqual(len(workflows), 10)

        workflows = list(self.cl.workflow_list(page=1, limit=1))
        self.assertEqual(len(workflows), 1)
        self.assertEqual(workflows[0].title, "workflow 9")

        workflows = list(self.cl.workflow_list(page=10, limit=1))
        self.assertEqual(len(workflows), 1)
        self.assertEqual(workflows[0].title, "workflow 0")

    @parameterized.expand([(2,), (4,)])
    def test04_wait_for_workflow(self, timeout):
        task = Task()
        task.execute(time.sleep, 3)
        workflow_uid = self.cl.enqueue([[task]])

        if timeout < 3:
            with self.assertRaises(Timeout):
                self.cl.workflow_wait(workflow_uid, timeout=timeout)
        else:
            result = self.cl.workflow_wait(workflow_uid, timeout=6)
            self.assertTrue(result.is_done)

    def test05_delete_workflow(self):
        task = Task()
        task.execute(time.sleep, 3)
        workflow_uid = self.cl.enqueue([[task]])

        with self.assertRaises(InvalidAction):
            self.cl.workflow_delete(workflow_uid)

        self.cl.workflow_wait(workflow_uid)
        self.cl.workflow_delete(workflow_uid)

        with self.assertRaises(NotFound):
            self.cl.workflow_get(workflow_uid)

    def test06_delete_workflow_task(self):
        task = Task()
        task.execute(time.sleep, 0.1)
        workflow_uid = self.cl.enqueue([[task]])
        result = self.cl.workflow_wait(workflow_uid)
        task = result.stages[0][0]

        with self.assertRaises(InvalidAction):
            self.cl.task_delete(task.uid)

    @parameterized.expand([("in_parallel",), ("in_series",), ("in_series_and_in_parallel",)])
    def test01_multiple_tasks(self, name):
        task_1 = Task()
        task_1.execute(time.sleep, 1)

        task_2 = Task()
        task_2.execute(time.sleep, 1)

        task_3 = Task()
        task_3.execute(time.sleep, 1)

        if name == "in_parallel":
            workflow_uid = self.cl.enqueue([[task_1, task_2, task_3]])
            result = self.cl.workflow_wait(workflow_uid)
            self.assertEqual(len(result.stages), 1)
            self.assertAlmostEqual(result.done_at - result.started_at, 1, delta=0.1)

        elif name == "in_series":
            workflow_uid = self.cl.enqueue([[task_1], [task_2], [task_3]])
            result = self.cl.workflow_wait(workflow_uid)
            self.assertEqual(len(result.stages), 3)
            self.assertAlmostEqual(result.done_at - result.started_at, 3, delta=0.1)
        else:

            workflow_uid = self.cl.enqueue([[task_1, task_2], [task_3]])
            result = self.cl.workflow_wait(workflow_uid)
            self.assertEqual(len(result.stages), 2)
            self.assertAlmostEqual(result.done_at - result.started_at, 2, delta=0.1)

        result = self.cl.workflow_wait(workflow_uid)
        self.assertEqual(result.status, WorkflowStatus.PASSED)
        for stage in result.stages:
            for task in stage:
                self.assertEqual(task.status, TaskStatus.PASSED)

    @parameterized.expand(
        [
            ("always", True, True),
            ("always", True, False),
            ("always", False, True),
            ("always", False, False),
            ("on_success", True, True),
            ("on_success", True, False),
            ("on_success", False, True),
            ("on_success", False, False),
            ("on_failure", True, True),
            ("on_failure", True, False),
            ("on_failure", False, True),
            ("on_failure", False, False),
        ]
    )
    def test04_task_conditions(self, when, is_prior_task_succeeded, allow_failure):
        task_1 = Task()
        task_1.allow_failure = allow_failure
        task_1.execute(time.sleep, 1 if is_prior_task_succeeded else "s")

        task_2 = Task()
        task_2.when = Conditions(when)
        task_2.execute(time.sleep, 1)

        workflow_uid = self.cl.enqueue([[task_1], [task_2]])
        result = self.cl.workflow_wait(workflow_uid)

        task_2 = result.stages[1][0]
        task_2 = self.cl.task_get(task_2.uid)

        if when == "always":
            self.assertEqual(task_2.status, TaskStatus.PASSED)

        elif when == "on_success":
            if is_prior_task_succeeded or allow_failure:
                self.assertEqual(task_2.status, TaskStatus.PASSED)
            else:
                self.assertEqual(task_2.status, TaskStatus.SKIPPED)
                self.assertNotEqual(task_2.skipped_at, None)

        elif when == "on_failure":
            if is_prior_task_succeeded or allow_failure:
                self.assertEqual(task_2.status, TaskStatus.SKIPPED)
                self.assertNotEqual(task_2.skipped_at, None)
            else:
                self.assertEqual(task_2.status, TaskStatus.PASSED)
