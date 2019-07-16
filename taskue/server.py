import time
import pickle
from redis import Redis
from taskue import Task
from taskue import WorkflowItem as Workflow
from taskue import (
    logging,
    RedisKeys,
    WorkflowStatus,
    WorkflowStageStatus,
    TaskStatus,
    RunnerStatus,
    decode_hash,
)


class TaskueServer:
    def __init__(self, redis: Redis):
        """TaskRunner server
        
        Arguments:
            redis {Redis} -- redis connection object
        """
        self._redis = redis
        self._redis.set_response_callback("HGETALL", decode_hash)

    def _get_task(self, uid):
        return pickle.loads(self._redis.get(RedisKeys.TASK.format(uid)))

    def _get_workflow(self, uid):
        blob = self._redis.get(RedisKeys.WORKFLOW.format(uid))
        workflow = Workflow(**pickle.loads(blob))
        return workflow

    def _save_workflow(self, workflow):
        blob = pickle.dumps(workflow.__dict__)
        rkey = RedisKeys.WORKFLOW.format(workflow.uid)
        self._redis.set(rkey, blob)

    def _save_task(self, task):
        blob = pickle.dumps(task)
        rkey = RedisKeys.TASK.format(task.uid)
        self._redis.set(rkey, blob)

    def _start_current_stage_tasks(self, workflow, prev_stage_status=None):
        logging.info(
            "Scheduling stage %s of workflow %s", workflow.current_stage, workflow.uid
        )
        for uid in workflow.current_stage_tasks:
            logging.info("Scheduling task %s", uid)
            task = self._get_task(uid)
            if prev_stage_status == WorkflowStageStatus.FAILED and task.skip_on_failure:
                task.status = TaskStatus.SKIPPED
            else:
                self._start_task(task)

            workflow.update_task_status(task)

    def _start_task(self, task):
        task.status = TaskStatus.QUEUED
        task.queued_at = time.time()
        self._redis.rpush(RedisKeys.QUEUE.format(task.tag), pickle.dumps(task))

    def start(self):
        logging.info("Taskue server is running ...")
        queues = [RedisKeys.PENDING, RedisKeys.TASK_EVENTS]
        while True:
            queue, blob = self._redis.blpop(queues)           
            if queue.decode() == RedisKeys.PENDING:
                workflow = Workflow(**pickle.loads(blob))
                workflow.status = WorkflowStatus.RUNNING
                workflow.started_at = time.time()
                self._start_current_stage_tasks(workflow)
                self._save_workflow(workflow)

            elif queue.decode() == RedisKeys.TASK_EVENTS:
                task = pickle.loads(blob)
                workflow = self._get_workflow(task.workflow_uid)
                workflow.update_task_status(task)
                status = workflow.get_stage_status(workflow.current_stage)
                if status in WorkflowStageStatus.DONE_STATES:
                    if workflow.is_last_stage():
                        workflow.done_at = time.time()
                        workflow.update_status()
                    else:
                        workflow.current_stage += 1
                        self._start_current_stage_tasks(
                            workflow, prev_stage_status=status
                        )
                self._save_workflow(workflow)