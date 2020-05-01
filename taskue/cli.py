import os
import sys
import pathlib
import json
import yaml
import click
import toml
import math
from tabulate import tabulate
from datetime import datetime
import redis
from taskue import Taskue, NotFound, InvalidAction, Timeout
from taskue.runner import TaskueRunner
import jinja2

DEFAULT_CONFIG = dict(redis_host="127.0.0.1", redis_port=6379, redis_secret=None, namespace="default")

CONFIG_PATH = os.path.expanduser("~/.taskue.toml")
pathlib.Path(CONFIG_PATH).touch(exist_ok=True)





status_color_map = {
    "running": "blue",
    "passed": "green",
    "failed": "red",
    "errored": "red",
    "timedout": "red",
    "pending": "yellow",
}


def load_config():
    content = pathlib.Path(CONFIG_PATH).read_text()
    try:
        return toml.loads(content)
    except toml.TomlDecodeError:
        return False


def save_config(config, validate=True):
    if validate:
        validate_config(config)

    pathlib.Path(CONFIG_PATH).write_text(toml.dumps(config))


def color_status(status):
    return click.style(status.capitalize(), fg=status_color_map.get(status))


def validate_config(config):
    try:
        assert config.get("redis_host")
        assert config.get("redis_port") and type(config.get("redis_port")) is int
        assert config.get("namespace")
    except AssertionError:
        fail("Invalid configuration")


def success(message):
    click.echo(click.style("Namespace [default]", bg="yellow", fg="black"))
    click.echo(click.style(message, fg="white"))
    sys.exit()


def fail(message):
    click.echo(click.style("error: %s" % message, fg="bright_red"), err=True)
    sys.exit(1)


def to_yes_or_no(value):
    return "yes" if value else "no"


def to_datetime(timestamp):
    if timestamp:
        time = datetime.fromtimestamp(timestamp)
        return time.strftime("%Y-%m-%d %H:%M:%S")


jinja_env = jinja2.Environment()
jinja_env.filters["color_status"] = color_status
jinja_env.filters["to_yes_or_no"] = to_yes_or_no
jinja_env.filters["datetime"] = to_datetime


task_template = jinja_env.from_string(
"""
uid                 {{task.uid}}
title               {{task.title}}
retries             {{task.retries}}
when                {{task.when.value}}
tag                 {{task.tag}}
timeout             {{task.timeout}}
allowed to fail     {{task.allow_failure | to_yes_or_no}}
status              {{task.status.value | color_status}}
attempts            {{task.attempts}}
rescheduleds        {{task.rescheduleded}}
result              {{task.result}}
runner              {{task.runner}}
created at          {{task.created_at | datetime}}
queued at           {{task.queued_at | datetime}}
started at          {{task.started_at | datetime}}
executed at         {{task.executed_at | datetime}}
skipped at          {{task.skipped_at | datetime}}
terminated at       {{task.terminated_at | datetime}}
""".strip()
)

workflow_template = jinja_env.from_string(
"""
uid             {{workflow.uid}}
title           {{workflow.title}}
status          {{workflow.status.value | color_status}}
created at      {{workflow.created_at | datetime}}
started at      {{workflow.started_at | datetime}}
done at         {{workflow.done_at | datetime}}
stages               
    {%- for stage in workflow.stages %}
    stage {{loop.index}}
    {%- for task in stage %}
     - uid                 {{task.uid}}
       title               {{task.title}}
       status              {{task.status.value | color_status}}
       allowed_to_fail     {{task.allow_failure | to_yes_or_no}}
    {%- endfor -%}
    {%- endfor -%}
""".strip()
)





@click.group()
@click.option("--namespace", "-ns", default=None, type=str, help="Namespace")
@click.pass_context
def cli(ctx=None, namespace=None):
    if ctx.invoked_subcommand in ("config"):
        return

    ctx.obj = dict()
    config = load_config()
    ctx.obj["config"] = config
    ctx.obj["namespace"] = namespace or config.get("namespace", "default")
    ctx.obj["redis"] = redis.Redis(
        host=config["redis_host"], port=config["redis_port"], password=config.get("redis_secret")
    )

    try:
        ctx.obj["redis"].ping()
    except redis.exceptions.ConnectionError:
        fail("Cannot connect to redis on {redis_host}:{redis_port}".format(**config))

    ctx.obj["taskue"] = Taskue(ctx.obj["redis"], ctx.obj["namespace"])


@cli.group()
@click.pass_context
def config(ctx):
    """Manage config"""
    pass


@cli.group()
@click.pass_context
def namespace(ctx):
    """Manage namespaces"""
    pass


@cli.group()
@click.pass_context
def runner(ctx):
    """start and list runners"""
    pass


@cli.group()
@click.pass_context
def workflow(ctx):
    """list, get, wait and delete workflows"""
    pass


@cli.group()
@click.pass_context
def task(ctx):
    """list, get and wait tasks"""
    pass


## Config


@config.command(name="show", help="Show current configuration")
@click.pass_context
def config_show(ctx):
    config = load_config()
    if config:
        click.echo("file path %s" % CONFIG_PATH)
        click.echo(tabulate(config))
    else:
        fail("invalid config, do taskue config reset to fix it")


@config.command(name="set", help="Sets an individuals value in configuration")
@click.option("--redis-host", "-h", type=str, help="Redis hostname")
@click.option("--redis-port", "-p", type=int, help="Redis port")
@click.option("--redis-secret", "-s", type=int, help="Redis secret")
@click.pass_context
def config_set(ctx, redis_host, redis_port, redis_secret):
    config = load_config()
    config.update(
        redis_host=redis_host or config.get("redis_host"),
        redis_port=redis_port or config.get("redis_port"),
        redis_secret=redis_secret or config.get("redis_secret"),
    )
    save_config(config)


@config.command(name="edit", help="Edit configuration from the editor")
@click.pass_context
def config_edit(ctx):
    config = pathlib.Path(CONFIG_PATH).read_text()
    changes = click.edit(text=config, require_save=True, extension=".toml")
    if changes:
        config = toml.loads(changes)
        save_config(config)
        success("configuration is updated")


@config.command(name="reset", help="Reset configuration to the default")
@click.pass_context
def config_reset(ctx):
    save_config(DEFAULT_CONFIG, validate=False)
    success("configuration is updated")


## Namespace


@namespace.command(name="list", help="List namespaces")
@click.pass_context
def namespace_list(ctx):
    results = []
    namespaces = ctx.obj["taskue"].namespace_list()
    for namespace in namespaces:
        results.append({"NAME": namespace["name"], "CREATED AT": to_datetime(namespace["timestamp"])})
    click.echo(tabulate(results))


@namespace.command(name="get", help="Get current namespace")
@click.pass_context
def namespace_get(ctx):
    click.echo(ctx.obj["namespace"])


@namespace.command(name="switch", help="Switch to namespace")
@click.argument("name", type=str)
@click.pass_context
def namespace_switch(ctx, name):
    ctx.obj["config"].update(namespace=name)
    save_config(ctx.obj["config"])
    success("switched to namespace %s" % name)


@namespace.command(name="delete", help="Delete namespace")
@click.argument("name", type=str)
@click.confirmation_option(prompt="Are you sure you want to delete the namespace?")
@click.pass_context
def namespace_delete(ctx, name):
    ctx.obj["taskue"].namespace_delete(name)
    success("namespace is deleted")


## Runner


@runner.command(name="start", help="Start new taskue runner")
@click.option("--name", "-n", default=None, type=str, help="Runner name (should be unique)")
@click.option("--timeout", "-t", default=3600, type=int, help="Runner default timeout")
@click.option("--queues", "-q", multiple=True, type=str, default=None)
@click.option("--run-untaged-tasks", "-a", is_flag=True)
@click.pass_context
def runner_start(ctx, name, queues, timeout, run_untaged_tasks):
    runner = TaskueRunner(
        ctx.obj["redis"],
        namespace=ctx.obj["namespace"],
        name=name,
        queues=queues,
        timeout=timeout,
        run_untaged_tasks=run_untaged_tasks,
    )
    runner.start()


@runner.command(name="list", help="List all runners")
@click.pass_context
def runner_list(ctx):
    results = []
    client = ctx.obj["taskue"]
    for runner in client.runner_list():
        results.append(
            {
                "NAME": runner["name"],
                "STATUS": runner["status"],
                "QUEUES": runner["queues"],
                "TIMEOUT": runner["timeout"],
            }
        )
    click.echo(tabulate(results))


## Workflow

@workflow.command(name="list", help="List workflows")
@click.pass_context
@click.option("--page", "-p", default=1, type=int, help="page number")
@click.option("--limit", "-l", default=50, type=int, help="results per page")
@click.option("--output", "-o", type=click.Choice(["json", "yaml"]))
def workflow_list(ctx, page, limit, output):
    items = []
    for workflow in ctx.obj["taskue"].workflow_list(page=page, limit=limit):
        items.append(
            (workflow.uid, workflow.title, color_status(workflow.status.value), to_datetime(workflow.created_at))
        )
    click.echo(tabulate(items, ("UID", "TITLE", "STATUS", "CREATED AT")))


@workflow.command(name="get", help="Get workflow info")
@click.argument("uid", type=str)
@click.option("--output", "-o", type=click.Choice(["json", "yaml"]))
@click.pass_context
def workflow_get(ctx, uid, output):
    workflow = ctx.obj["taskue"].workflow_get(uid)
    if json:
        click.echo(json.dumps(workflow, default=lambda o: o.__dict__, indent=4))
    else:
        click.echo(workflow_template.render(workflow=workflow).replace('None', click.style('none', dim=True)))


@workflow.command(name="wait", help="Wait for workflow to finish")
@click.argument("uid", type=str)
@click.option("--timeout", "-t", default=60, type=int, help="timeout in seconds")
@click.pass_context
def workflow_wait(ctx, uid, timeout):
    try:
        ctx.obj["taskue"].workflow_wait(uid, timeout)
    except Exception as e:
        fail(str(e))
    else:
        success("workflow is finished")


@workflow.command(name="delete", help="Delete workflow (it must be finished)")
@click.argument("uid", type=str)
@click.pass_context
def workflow_delete(ctx, uid):
    try:
        ctx.obj["taskue"].workflow_delete(uid)
    except NotFound as e:
        return fail(str(e))

    success("workflow is deleted")


## Task

@task.command(name="list", help="List tasks")
@click.pass_context
@click.option("--page", "-p", default=1, type=int, help="page number")
@click.option("--limit", "-l", default=50, type=int, help="results per page")
@click.option("--output", "-o", type=click.Choice(["json", "yaml"]))
def task_list(ctx, page, limit, output):
    items = []
    for task in ctx.obj["taskue"].task_list(page=page, limit=limit):
        items.append(
            (task.uid, task.title, color_status(task.status.value), task.workflow, to_datetime(task.created_at))
        )
    click.echo(tabulate(items, ("UID", "TITLE", "STATUS", "WORKFLOW", "CREATED AT")))


@task.command(name="get", help="Get task info")
@click.argument("uid", type=str)
@click.option("--output", "-o", type=click.Choice(["json", "yaml"]))
@click.pass_context
def task_get(ctx, uid, output):
    try:
        task = ctx.obj["taskue"].task_get(uid)
    except NotFound as e:
        return fail(e)

    if output:
        if output == "json":
            click.echo(json.dumps(task.json, indent=4))
        elif output == "yaml":
            click.echo(yaml.dump(task.json, indent=4))
    else:
        click.echo(task_template.render(task=task))


@task.command(name="wait", help="Wait for task to finish")
@click.argument("uid", type=str)
@click.option("--timeout", "-t", default=60, type=int, help="timeout in seconds")
@click.pass_context
def task_wait(ctx, uid, timeout):
    try:
        ctx.obj["taskue"].task_wait(uid, timeout)
    except Exception as e:
        fail(e)
    else:
        success("task is finished")


@task.command(name="delete", help="Delete task")
@click.argument("uid", type=str)
@click.pass_context
def task_delete(ctx, uid):
    try:
        ctx.obj["taskue"].task_delete(uid)
    except IndentationError as e:
        return fail(e)

    success("task is deleted")


if __name__ == "__main__":
    cli()
