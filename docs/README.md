# Taskue Documentation

Multi stages task queue uses Redis as a backend.

## Installation

Install using pip

```bash
pip install taskue
```

or install from source 
```bash
git clone https://github.com/ahelsayd/taskue; cd taskue
python3 -m setup.py
```

## Getting Started

Create client 

```python
from redis import Redis
from taskue import Taskue, Task

taskue = Taskue(Redis())
```

queue single task

```python
task = Task()
task.execute(time.sleep, 1)

# execute task
task_id = taskue.run(task)

# wait for the task to finish
result = taskue.task_wait(task_id)
```

or queue multiple tasks as a workflow

```python
# define 3 tasks
task_1 = Task()
task_1.execute(time.sleep, 1)

task_2 = Task()
task_2.execute(time.sleep, 1)

task_3 = Task()
task_3.execute(time.sleep, 1)

# execute task_1 and task_2 in parallel then execute task_3
workflow_id = taskue.run_workflow([[task_1, task_2], [task_3]])

# wait for the workflow to finish
result = taskue.workflow_wait(workflow_id)
```


## Features

### Workflows

### Namespaces

### Execution Conditions

### Runner Monitoing

### Task Re-Schedling 

### Auto Modules Loader


<!-- - Run a single task.
- Run multiple tasks (in parallel/series) as a workflow.
- Retry on fail for a configured number of retires.
- Capture task logs.
- Task can be taged with label so it will run only on runners that has the same tag. 
- Skip task if any task of the previous stages failed.
 -->
