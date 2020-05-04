# Taskue Documentation

**Taskue** (Task Queue) is a powerful library for queueing tasks / workflows and processing with one or more runners.
It is using Redis as a backend.


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

Import *Taskue*

```python
from redis import Redis
from taskue import Taskue, Task

taskue = Taskue(Redis())
```

Enqueue single task

```python
task = Task()
task.enqueue(time.sleep, 1)

# execute task
task_id = taskue.enqueue(task)

# wait for the task to finish
result = taskue.task_wait(task_id)
```

Enqueue multiple tasks as a workflow

```python
# define 3 tasks
task_1 = Task()
task_1.execute(time.sleep, 1)

task_2 = Task()
task_2.execute(time.sleep, 1)

task_3 = Task()
task_3.execute(time.sleep, 1)

# execute task_1 and task_2 in parallel then execute task_3
workflow_id = taskue.enqueue([[task_1, task_2], [task_3]])

# wait for the workflow to finish
result = taskue.workflow_wait(workflow_id)
```

### Start a Runner

To start executing queued tasks / worklfows you need to start one or more runners.

```bash
taskue runner start 
```


## Namespaces
Namespaces allows you to run multiple groups of runners and use the same redis server
so for example you can have *production*, *staging* and *testing* namespaces.

?> See [Command Line Interface](/cli?id=namespace) commands for Namespaces.

```python
taskue = Taskue(redis_conn=Redis(), namespace="testing")
```

```bash
taskue runner start --name myrunner --namespace testing 
```


## Tasks

?> See [Command Line Interface](/cli?id=task) commands for Tasks.

### Parameters
Tasks has the following parameters:

- `title`
- `retries`
- `tag`
- `timeout`
- `allow_failure`
- `allow_rescheduling`
- `when`

<!-- <dl>
  <dt><code>title</code></dt>
  <dd>Gives the task a meaningful title</dd>
  <dt><code>retries</code></dt>
  <dd>Allows you to configure how many times a task is going to be retried in case of a failure.</dd>
  <dt><code>tag</code></dt>
  <dd>Used to select specific Runners to run this task.</dd>
  <dt><code>timeout</code></dt>
  <dd>Allows you to configure task execution timeout.</dd>
  <dt><code>allow_failure</code></dt>
  <dd>Allows you to configure task execution timeout.</dd>
  <dt><code>allow_rescheduling</code></dt>
  <dd>
    allows job to fail without affecting the workflow.
    > only used when the task is a part of a workflow.
  </dd>
  <dt><code>when</code></dt>
  <dd>Allows you to configure task execution timeout.</dd>
</dl> -->


    - gives the task a meaningful title
- `retries`
    <!-- - allows you to configure how many times a task is going to be retried in case of a failure. -->
- `tag`
    <!-- - is used to select specific Runners to run this task. -->
- `timeout`
    <!-- - allows you to configure task execution timeout. -->
- `allow_failure`
    <!-- - allows job to fail without affecting the workflow. -->
    <!-- - it is only used when the task is a part of a workflow. -->
- `allow_rescheduling`
- `when`
    <!-- - `Conditions.ALWAYS`
    - `Conditions.ON_SUCCESS`
    - `Conditions.ON_FAILURE` -->


### Enqueue Tasks
```python
task = Task(title="mytask")
task.execute(time.time, 1)

task_uid = taskue.enqueue(task)
result = taskue.task_wait(task_uid)
```

### Retrieving Tasks
```python
# get task by its id
task = taskue.task_get(task_id)
# list tasks, get first page and limit result by 25 result
tasks = taskue.tasks_list(page=1, limit=25)
# get number of tasks
number_of_tasks = taskue.tasks_count()
```

### Delete Task
```python
taskue.task_delete(task_id)
```

## Workflows
- Workflow consists of stages and each stage can has one or more task.
- Tasks of the same stage are run in parallel.
- Tasks of the next stage are run after the tasks of the the previous stage complete successfully.

?> See [Command Line Interface](/cli?id=workflow) commands for Workflows.

### Enqueue Workflows
tasks *task_1* and *task_2* are in the same stage and will run in parallel, then task *task_3* will run if *task_1* and *task_2* complete successfully.
```python
workflow_id = taskue.enqueue([[task_1, task_2], [task_3]], title="myworkflow")
result = taskue.workflow_wait(workflow_id)
```

### Retrieving Workflows
```python
# get workflow by its id
workflow = taskue.workflow_get(workflow_id)
# list workflows, get first page and limit result by 25 result
workflows = taskue.workflow_list(page=1, limit=25)
# get number of workflows
number_of_workflows = taskue.workflow_count()
```

### Delete Workflow
You can delete only finished workflows from the system
```python
taskue.workflow_delete(workflow_id)
```


## Runners
A Runner is a python process which runs in the background and executes the tasks that are queued to its queues. <br> Runners process a single task at a time, so if you want to run multiple tasks at the same time you need to start more runners.

?> See [Command Line Interface](/cli?id=runner) commands for Runners.


### Parameters
Runners has the following parameters:

- `name`
- `namespace`
- `queues`
- `timeout`
- `run_untagged_tasks`
- `auto_load_modules`


### Starting Runners
Using the [Command Line Interface](/cli?id=runner)
```bash
taskue runner start -n myrunner -q myqueue
```

Using python
```python
from taskue.runner import TaskueRunner

runner = TaskueRunner(name="myrunner", queues=["myqueue"])
runner.start()
```

### Heartbeat and Monitoring
- Runners periodically send heartbeat with timeout to prove they are still alive. 
- Each runner monitors the other runners. If it find runner with expired heartbeat, it will mark the runner as **dead** and if the runner was busy with executing a task it will reschdule its task again if the task `allow_rescheduling` option is enabled.

!> aaaaa



<!-- Runner sends heartbeat every 30 seconds or just before executing a task, this heartbeat is used to deter -->
<!-- Each runner monitors all the runners running in the same Namespace. -->


### Auto module Loading


<!-- Runners has the following parameters:
- `name`
- `namespace`
- `queues`
- `timeout`
- `run_untagged_tasks`
- `auto_load_modules`

### Starting a runner


or inside python process

```python
from taskue.runner import TaskueRunner

runner = TaskueRunner(name="myrunner", queues=["ubuntu16", "ubuntu:18"])
runner.start()
``` -->


<!-- ## Features

### Multi stages Workflows


### Namespaces

Namespaces allows you to run multiple group of runner(s).

### Execution Conditions

### Runner Monitoing

### Task Re-Schedling 

### Auto Modules Loader -->


<!-- - Run a single task.
- Run multiple tasks (in parallel/series) as a workflow.
- Retry on fail for a configured number of retires.
- Capture task logs.
- Task can be taged with label so it will run only on runners that has the same tag. 
- Skip task if any task of the previous stages failed.
 -->
