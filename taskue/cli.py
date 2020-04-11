import os
import click
from datetime import datetime
from redis import Redis
from taskue import Taskue, WorkflowNotFound, TaskNotFound
from taskue.server import TaskueServer
from taskue.runner import TaskueRunner


redis_conn = Redis()


def format_timestamp(timestamp):
    time = datetime.fromtimestamp(timestamp)
    return time.strftime("%Y-%m-%d %H:%M:%S")


def tabulate(data, prefix=20):
    result = ""
    if isinstance(data, dict):
        sformat = "{header:<{prefix}} {value}\n"
        for key, value in data.items():
            result += sformat.format(header=key, value=value, prefix=prefix)

    elif isinstance(data, list):
        if data:
            sformat = "{:<{prefix}}" * len(data[0])
            result += sformat.format(*data[0].keys(), prefix=prefix)
            result += "\n"

        for item in data:
            result += sformat.format(*item.values(), prefix=prefix)
            result += "\n"

    return result


@click.group()
@click.option("--redis-host", default="localhost", help="redis hostname")
@click.option("--redis-port", default=6379, help="redis port")
@click.pass_context
def main(ctx=None, redis_host=None, redis_port=None):
    ctx.obj = {}
    ctx.obj["redis"] = redis_conn
    ctx.obj["taskue"] = Taskue(redis_conn)


@main.group()
@click.pass_context
def server(ctx):
    pass


@main.group()
@click.pass_context
def runner(ctx):
    pass


@main.group()
@click.pass_context
def workflow(ctx):
    pass


@main.group()
@click.pass_context
def task(ctx):
    pass


@server.command(name="start")
@click.pass_context
def server_start(ctx):
    redis = ctx.obj["redis"]
    TaskueServer(redis).start()


@runner.command(name="start")
@click.pass_context
def runner_start(ctx):
    redis = ctx.obj["redis"]
    TaskueRunner(redis).start()


@runner.command(name="list")
@click.pass_context
def runner_list(ctx):
    results = []
    client = ctx.obj["taskue"]
    for uid in client.runner_list():
        runner = client.runner_get(uid)
        results.append(
            {
                "UID": runner["uid"],
                "NAME": runner["name"],
                "STATUS": runner["status"],
                "TAGS": runner["tags"],
                "TIMEOUT": runner["timeout"],
            }
        )
    click.echo(tabulate(results))


@workflow.command(name="list")
@click.pass_context
def workflow_list(ctx):
    results = []
    client = ctx.obj["taskue"]
    for uid in client.workflow_list():
        workflow = client.workflow_get(uid)
        results.append(
            {
                "UID": workflow.uid,
                "TITLE": workflow.title,
                "STATUS": workflow.status.value,
                "CREATED AT": format_timestamp(workflow.created_at),
            }
        )
    click.echo(tabulate(results))


@workflow.command(name="get")
@click.argument("uid")
@click.pass_context
def workflow_get(ctx, uid):
    client = ctx.obj["taskue"]
    try:
        workflow = client.workflow_get(uid)
    except WorkflowNotFound as e:
        return click.echo(str(e), err=True)

    click.echo(
        tabulate(
            {
                "UID": workflow.uid,
                "TITLE": workflow.title,
                "STATUS": workflow.status.value,
                "CREATED AT": format_timestamp(workflow.created_at),
                "STARTED AT": format_timestamp(workflow.started_at),
                "DONE AT": format_timestamp(workflow.done_at) if workflow.done_at else "",
            }
        )
    )

    tasks = []
    for stage in workflow.stages:
        for task in stage:
            tasks.append(
                {
                    "UID": task.uid,
                    "TITLE": task.title,
                    "STAGE": task.stage + 1,
                    "STATUS": task.status.value,
                    "ALLOWED TO FAIL": task.allow_failure,
                }
            )

    click.echo("TASKS", nl=True)
    click.echo(tabulate(tasks))


@workflow.command(name="delete")
@click.argument("uid")
@click.pass_context
def workflow_delete(ctx, uid):
    client = ctx.obj["taskue"]
    try:
        client.workflow_delete(uid)
    except WorkflowNotFound as e:
        return click.echo(str(e), err=True)

    click.echo("workflow is deleted")


@task.command(name="get")
@click.argument("uid")
@click.pass_context
def task_get(ctx, uid):
    client = ctx.obj["taskue"]
    try:
        task = client.task_get(uid)
    except TaskNotFound as e:
        return click.echo(str(e), err=True)

    click.echo(tabulate({"UID": task.uid, "TITLE": task.title, "STATUS": task.status.value,}, 15))


def cli():
    main()


if __name__ == "__main__":
    cli()
