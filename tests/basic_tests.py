import time
from taskue import Task, WorkflowStatus, TaskStatus
from tests import BaseTestCase

class BasicTests(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test01(self):
        task_1 = Task()
        task_1.execute(time.sleep, 1)
        
        task_2 = Task()
        task_2.execute(time.sleep, 1)

        task_3 = Task()
        task_3.execute(time.sleep, 1)

        workflow_uid = self.cl.run([[task_1, task_2], [task_3]])

        time.sleep(4)

        workflow = self.cl.workflow_get(workflow_uid)
        self.assertEqual(workflow.status, WorkflowStatus.PASSED)
        for stage in workflow.stages.values():
            for task_status in stage.values():
                self.assertEqual(task_status, TaskStatus.PASSED)
