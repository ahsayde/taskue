import time
import pickle
from taskue import RedisKeys, WorkflowItem, Workflow, WorkflowStatus


class Client:
    def __init__(self, redis):
        self._redis = redis

    def schedule_workflow(self, workflow: Workflow):
        """Schedule workflow
        
        Arguments:
            workflow {Workflow} -- workflow object
        
        Returns:
            str -- unique id of the workflow
        """
        workflow.created_at = time.time()
        workflow_obj, tasks_objs = workflow.dump()

        for task in tasks_objs:
            self._redis.set(RedisKeys.TASK.format(task.uid), pickle.dumps(task))

        self._redis.rpush(RedisKeys.PENDING, pickle.dumps(workflow_obj))
        return workflow.uid

    def get_workflow_info(self, uid: str):
        """Gets workflow info
        
        Arguments:
            uid {str} -- workflow unique id
        
        Returns:
            dict -- workflow info
        """
        blob = self._redis.get(RedisKeys.WORKFLOW.format(uid))
        if blob:
            info = pickle.loads(blob)
            info.pop("current_stage")
            return info

    def get_task_info(self, uid: str):
        """Gets task info
        
        Arguments:
            uid {str} -- task unique id
        
        Returns:
            dict -- task info
        """
        blob = self._redis.get(RedisKeys.TASK.format(uid))
        if blob:
            return pickle.loads(blob).__dict__

    def wait(self, uid: str):
        """Block until workflow finish
        
        Arguments:
            uid {str} -- workflow unique id
        """
        while True:
            workflow = self.get_workflow_info(uid)
            if workflow["status"] in WorkflowStatus.DONE_STATES:
                break
