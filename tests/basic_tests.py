import time
from taskue import Task, WorkflowStatus, TaskStatus
from tests import BaseTestCase


class BasicTests(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test01(self):
        task_1 = Task()
        task_1.execute(int, "0")

        task_2 = Task()
        task_2.execute(int, "1")

        task_3 = Task()
        task_3.execute(int, "2")

        workflow_uid = self.cl.run([[task_1], [task_2], [task_3]])
        workflow = self.cl.wait_for_workflow(workflow_uid)

        self.assertEqual(workflow.status, WorkflowStatus.PASSED)
        for stage in workflow.stages:
            for task in stage:
                self.assertEqual(task.status, TaskStatus.PASSED)
