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

#### Start a runner
```bash
taskue runner start --redis-host <localhost> --redis-port <6379>
```
More options: 
- `tags`: Runner tags, taged tasks will run only on runners which has its tag
- `timeout`: Runner timeout, task timeout overwrites it (default 1 hour)
- `run_untaged_tasks`: Allow runner to run untaged tasks (enabled by default).
- `path`: add the path of a module to include.


#### How it works

```python
from redis import Redis
from taskue import Taskue, Task 

tskue = Taskue(Redis())

# define tasks
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

Execute single task
```python
# execute single task
task_uid = tskue.run(t1)
```

or execute multiple tasks as a workflow
```python

# run task t1 and task t2 in stage 1 and task t3 in stage 2
workflow_uid = tskue.run_workflow([[t1, t2], [t3]])
```

> Hint: All the tasks in the same stage run in parallel, and each stage starts when the previous stage finish.

## CLI
```bash
taskue --help
Usage: cli.py [OPTIONS] COMMAND [ARGS]...

Options:
  --redis-host TEXT     redis hostname
  --redis-port INTEGER  redis port
  --help                Show this message and exit.

Commands:
  runner
  task
  workflow
```

### commands
- `runner`:
  - `start`: start new runner
- `task`:
  - `list`: list all tasks
  - `get`: get task info using its uid
  - `logs` get task logs
  - `delete` delete task
- `workflow`
  - `list`: list all workflows
  - `get`: get workflow info using its uid
  - `logs` get workflow logs
  - `delete` delete workflow

## To Do
- [ ] Detect dead runner and reschedule its task.
- [ ] Add more docs and examples
- [ ] Add tests