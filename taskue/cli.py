import os
import pathlib
import click
import toml
from datetime import datetime
import redis
from taskue import Taskue, WorkflowNotFound, TaskNotFound
from taskue.server import TaskueServer
from taskue.runner import TaskueRunner


DEFAULT_CONFIG = dict(
    redis_host="127.0.0.1",
    redis_port=6379,
    redis_secret=None,
)

CONFIG_PATH = os.path.expanduser("~/.taskue.toml")
pathlib.Path(CONFIG_PATH).touch(exist_ok=True)


def load_config():
    content = pathlib.Path(CONFIG_PATH).read_text()
    try:
        return toml.loads(content)
    except toml.TomlDecodeError:
        return False


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

    return result.strip()


@click.group()
@click.pass_context
def cli(ctx=None):
    if ctx.invoked_subcommand in ("config", "namespace"):
        return

    ctx.obj = {}
    config = load_config()
    ctx.obj["config"] = config
    ctx.obj["redis"] = redis.Redis(
        host=config["redis_host"], port=config["redis_port"], password=config.get("redis_secret")
    )

    try:
        ctx.obj["redis"].ping()
    except redis.exceptions.ConnectionError:
        return click.echo("Cannot connect to redis @ {redis_host}:{redis_port}".format(**config), err=True)
    else:
        ctx.obj["taskue"] = Taskue(ctx.obj["redis"])


@cli.group()
@click.pass_context
def start(ctx):
    """Start resources, valid for (server, runner)"""
    pass


@cli.group()
@click.pass_context
def switch(ctx):
    """Switch to resources, valid for (namespace)"""
    pass


@cli.group()
@click.pass_context
def get(ctx):
    """Get reseources, valid for (workflow, task)"""
    pass


@cli.group(name="list")
@click.pass_context
def list_(ctx):
    """List resources, valid for (namespace, runner, workflow, task)"""
    pass


@cli.group()
@click.pass_context
def delete(ctx):
    """Delete resources, valid for (namespace, workflow)"""
    pass


@cli.group()
@click.pass_context
def wait(ctx):
    """Wait for resources to finish, valid for (workflow, task)"""
    pass


@cli.group()
@click.pass_context
def edit(ctx):
    """Edit resources, valid for (config)"""
    pass


@cli.group()
@click.pass_context
def reset(ctx):
    """Reset resources, valid for (config)"""
    pass


## Switch ################################################

@switch.command(name="namespace", help="switch to namespace")
@click.argument("name", type=str)
@click.pass_context
def namespace_switch(ctx, name):
    ctx.obj["namespace"] = name
    click.echo("switched to namespace %s" % name)


## Start #################################################

@start.command(name="server", help="start taskue server")
@click.pass_context
def server_start(ctx):
    redis = ctx.obj["redis"]
    TaskueServer(redis).start()


@start.command(name="runner", help="start new taskue runner")
@click.option("--name", "-n", default=None, type=str, help="Runner name (should be unique)")
@click.option("--timeout", "-i", default=3600, type=int, help="Runner default timeout")
@click.option("--tags", "-t", multiple=True, type=str, default=None)
@click.option('--run-untaged-tasks', is_flag=True)
@click.pass_context
def runner_start(ctx, name, tags, timeout, run_untaged_tasks):
    redis_conn = ctx.obj["redis"]
    runner = TaskueRunner(
        redis_conn, name=name, tags=tags, timeout=timeout, run_untaged_tasks=run_untaged_tasks
    )
    runner.start()


### Get #########################################################

@get.command(name="namespace", help="get current namespace")
@click.pass_context
def namespace_get(ctx):
    click.echo(ctx.obj["namespace"])


@get.command(name="config", help="get current configuration")
@click.pass_context
def config_get(ctx):
    config = load_config()
    if config:
        click.echo("file path %s" % CONFIG_PATH)
        click.echo(tabulate(config))
    else:
        click.echo("invalid config, do taskue config reset to fix it", err=True)


@get.command(name="workflow", help="get workflow info")
@click.argument("uid", type=str)
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


@get.command(name="task", help="get task info")
@click.argument("uid", type=str)
@click.pass_context
def task_get(ctx, uid):
    client = ctx.obj["taskue"]
    try:
        task = client.task_get(uid)
    except TaskNotFound as e:
        return click.echo(str(e), err=True)

    click.echo(tabulate({"UID": task.uid, "TITLE": task.title, "STATUS": task.status.value,}, 15))


### List #########################################################

@list_.command(name="namespace", help="list namespaces")
@click.pass_context
def namespace_list(ctx):
    results = []
    client = ctx.obj["taskue"]
    namespaces = client.namespace_list()
    for namespace in namespaces:
        results.append({
            "NAME": namespace["name"],
            "CREATED AT": format_timestamp(namespace["timestamp"])
        })

    click.echo(tabulate(results))


@list_.command(name="runner", help="list runners")
@click.pass_context
def runner_list(ctx):
    results = []
    client = ctx.obj["taskue"]
    for uid in client.runner_list():
        runner = client.runner_get(uid)
        results.append(
            {
                "NAME": runner["name"],
                "STATUS": runner["status"],
                "TAGS": runner["tags"],
                "TIMEOUT": runner["timeout"],
            }
        )
    click.echo(tabulate(results))


@list_.command(name="workflow", help="list workflows")
@click.pass_context
@click.option("--page", "-p", default=1, type=int, help="page number")
@click.option("--limit", "-l", default=25, type=int, help="results per page")
def workflow_list(ctx, page, limit):
    results = []
    client = ctx.obj["taskue"]
    for uid in client.workflow_list(page=page, limit=limit):
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


### Delete #########################################################

@delete.command(name="namespace", help="delete namespace")
@click.argument("name", type=str)
@click.pass_context
def namespace_delete(ctx, name):
    client = ctx.obj["taskue"]
    client.namespace_delete()
    click.echo("namespace is deleted")


@delete.command(name="workflow", help="delete workflow (it must be finished)")
@click.argument("uid", type=str)
@click.pass_context
def workflow_delete(ctx, uid):
    client = ctx.obj["taskue"]
    try:
        client.workflow_delete(uid)
    except WorkflowNotFound as e:
        return click.echo(str(e), err=True)

    click.echo("workflow is deleted")


### Wait ################################################

@wait.command(name="workflow", help="wait for workflow to finish")
@click.argument("uid", type=str)
@click.pass_context
def workflow_wait(ctx, uid):
    client = ctx.obj["taskue"]
    try:
        client.workflow_wait(uid)
    except WorkflowNotFound as e:
        return click.echo(str(e), err=True)

    click.echo("workflow is finished")


@wait.command(name="task", help="wait for task to finish")
@click.argument("uid", type=str)
@click.pass_context
def task_wait(ctx, uid):
    client = ctx.obj["taskue"]
    try:
        client.task_wait(uid)
    except TaskNotFound as e:
        return click.echo(str(e), err=True)

    click.echo("task is finished")


### Edit ################################################

@edit.command(name="config", help="edit configuration")
@click.pass_context
@click.option("--redis-host", "-h", default="localhost", type=str, help="Redis hostname")
@click.option("--redis-port", "-p", default=6379, type=int, help="Redis port")
@click.option("--redis-secret", "-s", default=None, type=int, help="Redis secret")
def config_edit(ctx, redis_host, redis_port, redis_secret):
    config = load_config()
    if not config:
        return click.echo("invalid config, please do taskue config reset to fix it", err=True)

    config.update(
        redis_host=redis_host or config.get("redis_host"),
        redis_port=redis_port or config.get("redis_port"),
        redis_secret=redis_secret or config.get("redis_secret")
    )
    pathlib.Path(CONFIG_PATH).write_text(toml.dumps(config))


### Reset ################################################

@reset.command(name="config", help="reset configuration")
@click.pass_context
def config_reset(ctx):
    pathlib.Path(CONFIG_PATH).write_text(toml.dumps(DEFAULT_CONFIG))


if __name__ == "__main__":
    cli()
