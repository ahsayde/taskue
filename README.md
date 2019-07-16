# TasKue
Multi stages task queue uses [Redis](https://redis.io) as a backend.

## Features
- Multi stages.
- Retry on fail for a defined number of retires.
- Capture task logs.
- Task can be taged with label so it will run only on runners that has the same tag. 
- Skip task if any task of the previous stages failed.

## Installation
```bash
pip install taskue
```

## Getting started
#### Server
Start **taskue** server from the cli 
```bash
taskue server --redis-host <localhost> --redis-port <6379>
```

#### Runner
Register a new runner
```bash
taskue runner --redis-host <localhost> --redis-port <6379>
```
More options: 
- `tags`: Runner tags, taged tasks will run only on runners which has its tag
- `timeout`: Runner timeout, task timeout overwrites it (default 1 hour)
- `run_untaged_tasks`: Allow runner to run untaged tasks (enabled by default).


#### Client
Import **taskue** client
```python
from redis import Redis
from taskue.client import Client                         
   
cl = Client(Redis()) 
```

Define tasks and the workflow

```python
from taskue import Task, Workflow

t1 = Task() 
t1.execute(print, "Hello from task 1")                           
t1.retries = 3  # retry up to 3 times on failure                         

t2 = Task()
t2.timeout = 5 # set task timeout to 5
t2.execute(print, "Hello from task 2")  

t3 = Task()
t3.skip_on_failure = True # skip this task if any task of the previous stage failed
t3.execute(print, "Hello from task 3") 
```            

Workflow can be defined in two ways:
- Using ```add_stage``` and ```add_task``` methods 
```python
workflow = Workflow()

# create a stage and add task t1 to it
workflow.add_stage([t1])

# add task t2 to stage 1
workflow.add_task([t2], 1)

# add another stage for task t3
workflow.add_stage([t3])
```

- Or by passing the stages as list of tasks objects list
```python
# run task t1 and task t2 in stage 1 and task t3 in stage 2
workflow = Workflow([[t1, t2], [t3]])
```
finally schedule the workflow and get the result of the workflow and the tasks
```python
# Schedule the workflow
cl.schedule_workflow(workflow) 

# Get workflow info
cl.get_workflow_info(workflow.uid)

# Get task info
cl.get_task_info(t1.uid)
```

> Hint: All the tasks in the same stage run in parallel, and each stage starts when the previous stage finish.


Example of workflow info
```python
{'uid': '499dd9850b70454ebf4c0d64c93fdb66',
 'stages': [{'9a4690a4dec742dcb1f364e4717e5637': 'Passed'},
  {'4045c69a959d42c1b788ad0e88d046f5': 'Passed',
   'a21b8a0b915e490b95cd292370603f1f': 'Passed'}],
 'status': 'Passed',
 'created_at': 1562957295.0063102,
 'started_at': 1562957295.0065968,
 'done_at': 1562957295.0167484}

```

Example of task info
```python
{'uid': '9a4690a4dec742dcb1f364e4717e5637',
 'stage': 1,
 'retries': 1,
 'tag': 'default',
 'timeout': 3600,
 'skip_on_failure': False,
 'func': <function print>,
 'args': ('Hello from task 1',),
 'kwargs': {},
 'result': None,
 'logs': [],
 'created_at': 1562957295.0025797,
 'started_at': 1562957295.0084898,
 'queued_at': 1562957295.0075643,
 'executed_at': 1562957295.009686,
 'workflow_uid': '499dd9850b70454ebf4c0d64c93fdb66',
 'runner_uid': '9b95b9e113414904bd16450fcdc02394',
 'rescheduled': 0,
 'status': 'Passed'}
```

## To Do
- [ ] Detect dead runner and reschedule its task.
- [ ] Add more docs and examples
- [ ] Add tests