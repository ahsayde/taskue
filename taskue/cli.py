import os
import click
from redis import Redis
import taskue
from datetime import datetime
from taskue.utils import RedisKeys
from taskue.runner import TaskueRunner


def print_task(task):
    data = task.__dict__
    data.pop("logs")
    for k, v in data.items():
        if k.startswith("_"):
            continue
        print("{0: <20} {1}".format(k, v))


def print_workflow(workflow):
    data = workflow.__dict__
    for k, v in data.items():
        if k.startswith("_"):
            continue
        print("{0: <20} {1}".format(k, v))


@click.group()
@click.option("--redis-host", default="localhost", help="redis hostname")
@click.option("--redis-port", default=6379, help="redis port")
@click.pass_context
def main(ctx=None, redis_host=None, redis_port=None):
    redis_conn = Redis(host=redis_host, port=redis_port)
    ctx.obj["redis"] = redis_conn
    ctx.obj["tskue"] = taskue.Taskue(redis_conn)


@main.group()
@click.pass_context
def runner(ctx):
    pass


@main.group()
@click.pass_context
def task(ctx):
    pass


@main.group()
@click.pass_context
def workflow(ctx):
    pass

@runner.command(name="start")
@click.option('--tags', default="")
@click.option('--timeout', default=3600, type=int)
@click.option('--run_untaged_tasks', default=True, type=bool)
@click.option('--path', default=None, type=str)
@click.pass_context
def start_runner(ctx, tags, timeout, run_untaged_tasks, path):
    tags = tags.strip().split(",")
    runner = TaskueRunner(
        ctx.obj["redis"],
        tags=tags,
        timeout=timeout,
        run_untaged_tasks=run_untaged_tasks,
        path=path
    )
    runner.start()


@task.command(name="list")
@click.pass_context
def list_task(ctx):
    iterator = ctx.obj["tskue"].list_tasks()
    cursor = 0
    for key in iterator:
        cursor += 1
        print(key.decode().replace(RedisKeys.TASK.format(""), ""))
        if cursor > 10:
            cursor = 0
            input("load more ...")


@task.command(name="get")
@click.argument("uid")
@click.pass_context
def get_task(ctx, uid):
    task = ctx.obj["tskue"].get_task_info(uid)
    print_task(task)


@task.command(name="logs")
@click.argument("uid")
@click.pass_context
def get_task_logs(ctx, uid):
    try:
        task = ctx.obj["tskue"].get_task_info(uid)
    except taskue.TaskNotFound as e:
        print(str(e))
        return

    for log in task.logs:
        print("{timestamp} - [{level}] - {msg}".format(
            msg=log["msg"],
            level=log["level"],
            timestamp=datetime.fromtimestamp(log["timestamp"]),
        ))


@task.command(name="delete")
@click.argument("uid")
@click.pass_context
def delete_task(ctx, uid):
    ctx.obj["tskue"].delete_task(uid)
    print("Task is deleted")


@workflow.command(name="list")
@click.pass_context
def list_workflow(ctx):
    iterator = ctx.obj["tskue"].list_workflows()
    cursor = 0
    for key in iterator:
        cursor += 1
        print(key.decode().replace(RedisKeys.WORKFLOW.format(""), ""))
        if cursor > 10:
            cursor = 0
            input("load more ...")


@workflow.command(name="get")
@click.argument("uid")
@click.pass_context
def get_workflow(ctx, uid):
    workflow = ctx.obj["tskue"].get_workflow_info(uid)
    print_workflow(workflow)


@workflow.command(name="delete")
@click.argument("uid")
@click.pass_context
def delete_workflow(ctx, uid):
    ctx.obj["tskue"].delete_workflow(uid)
    print("Workflow is deleted")


def cli():
    main(obj={})
