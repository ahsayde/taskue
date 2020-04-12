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
    if ctx.invoked_subcommand == "config":
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

    ctx.obj["taskue"] = Taskue(ctx.obj["redis"])


@cli.group()
@click.pass_context
def config(ctx):
    pass


@cli.group()
@click.pass_context
def server(ctx):
    pass


@cli.group()
@click.pass_context
def runner(ctx):
    pass


@cli.group()
@click.pass_context
def workflow(ctx):
    pass


@cli.group()
@click.pass_context
def task(ctx):
    pass


@config.command(name="get")
@click.pass_context
def config_show(ctx):
    config = load_config()
    if config:
        click.echo("file path %s" % CONFIG_PATH)
        click.echo(tabulate(config))
    else:
        click.echo("invalid config, do taskue config reset to fix it", err=True)


@config.command(name="edit")
@click.pass_context
@click.option("--redis-host", "-h", default="localhost", type=str, help="Redis hostname")
@click.option("--redis-port", "-p", default=6379, type=int, help="Redis port")
@click.option("--redis-secret", "-s", default=None, type=int, help="Redis secret")
def config_init(ctx, redis_host, redis_port, redis_secret):
    
    config = load_config()
    if not config:
        return click.echo("invalid config, please do taskue config reset to fix it", err=True)

    config.update(
        redis_host=redis_host or config.get("redis_host"),
        redis_port=redis_port or config.get("redis_port"),
        redis_secret=redis_secret or config.get("redis_secret")
    )
    pathlib.Path(CONFIG_PATH).write_text(toml.dumps(config))


@config.command(name="reset")
@click.pass_context
def config_reset(ctx):
    pathlib.Path(CONFIG_PATH).write_text(toml.dumps(DEFAULT_CONFIG))


@server.command(name="start")
@click.pass_context
def server_start(ctx):
    redis = ctx.obj["redis"]
    TaskueServer(redis).start()


@runner.command(name="start")
@click.option("--name", "-n", default=None, type=str, help="Runner name (should be unique)")
@click.option("--timeout", "-i", default=3600, type=int, help="Runner default timeout")
@click.option("--tags", "-t", multiple=True, type=str, default=None)
@click.option('--run-untaged-tasks', is_flag=True)
@click.pass_context
def runner_start(ctx, name, tags, timeout, run_untaged_tasks):
    print(name, timeout, tags, run_untaged_tasks)
    redis_conn = ctx.obj["redis"]
    runner = TaskueRunner(
        redis_conn, name=name, tags=tags, timeout=timeout, run_untaged_tasks=run_untaged_tasks
    )
    runner.start()


@runner.command(name="list")
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


@workflow.command(name="list")
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


@workflow.command(name="get")
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


@workflow.command(name="wait")
@click.argument("uid", type=str)
@click.pass_context
def workflow_wait(ctx, uid):
    client = ctx.obj["taskue"]
    try:
        client.workflow_wait(uid)
    except WorkflowNotFound as e:
        return click.echo(str(e), err=True)

    click.echo("workflow is finished")


@workflow.command(name="delete")
@click.argument("uid", type=str)
@click.pass_context
def workflow_delete(ctx, uid):
    client = ctx.obj["taskue"]
    try:
        client.workflow_delete(uid)
    except WorkflowNotFound as e:
        return click.echo(str(e), err=True)

    click.echo("workflow is deleted")


@task.command(name="get")
@click.argument("uid", type=str)
@click.pass_context
def task_get(ctx, uid):
    client = ctx.obj["taskue"]
    try:
        task = client.task_get(uid)
    except TaskNotFound as e:
        return click.echo(str(e), err=True)

    click.echo(tabulate({"UID": task.uid, "TITLE": task.title, "STATUS": task.status.value,}, 15))


@task.command(name="wait")
@click.argument("uid", type=str)
@click.pass_context
def task_wait(ctx, uid):
    client = ctx.obj["taskue"]
    try:
        client.task_wait(uid)
    except TaskNotFound as e:
        return click.echo(str(e), err=True)

    click.echo("task is finished")


if __name__ == "__main__":
    cli()
