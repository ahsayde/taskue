import os
import sys
import pathlib
import json
import yaml
import click
import functools
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


def validate_config(config):
    try:
        assert config.get("redis_host")
        assert config.get("redis_port") and type(config.get("redis_port")) is int
        assert config.get("namespace")
    except AssertionError:
        fail("Invalid configuration")


def success(message):
    click.echo(click.style(message, fg="white"))
    sys.exit()


def fail(message):
    click.echo(click.style("error: %s" % message, fg="bright_red"), err=True)
    sys.exit(1)


def color_status(status):
    return click.style(status.capitalize(), fg=status_color_map.get(status))


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

workflows_template = jinja_env.from_string(
"""
UID                 TITLE                   STATUS                                           CREATED AT
{%- for workflow in workflows %}
{{workflow.uid}}    {{workflow.title}}      {{workflow.status.value | color_status}}         {{workflow.done_at | datetime}}
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
@click.pass_context
def runner_start(ctx, name, queues, timeout):
    runner = TaskueRunner(
        ctx.obj["redis"],
        namespace=ctx.obj["namespace"],
        name=name,
        queues=queues,
        timeout=timeout,
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

@workflow.command(name="get", help="list or get workflow(s)")
@click.argument("uid", type=str, required=False)
@click.option("--page", "-p", default=1, type=int, help="page number")
@click.option("--limit", "-l", default=50, type=int, help="number of results per page")
@click.pass_context
def workflows_get(ctx, uid, page, limit):
    cl = ctx.obj["taskue"]
    if uid:
        try:
            workflow = ctx.obj["taskue"].workflow_get(uid)
        except NotFound as e:
            fail(e)

        success(workflow_template.render(workflow=workflow))
    else:
        workflows = []
        for workflow in cl.workflow_list(page=page, limit=limit):
            workflows.append(workflow)
        
        success(workflows_template.render(workflows=workflows))


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

@task.command(name="get", help="list or get  task(s)")
@click.argument("uid", type=str, required=False)
@click.option("--output", "-o", type=click.Choice(["json", "yaml"]), help="export results")
@click.option("--page", "-p", default=1, type=int, help="page number")
@click.option("--limit", "-l", default=50, type=int, help="number of results per page")
@click.pass_context
def task_get(ctx, uid, output, page, limit):
    cl = ctx.obj["taskue"]
    if uid:
        try:
            result = cl.task_get(uid)
        except NotFound as e:
            return fail(e)
    else:
        result = []
        for task in cl.task_list(page=page, limit=limit):
            result.append(task)

    success(result[0].__dict__)

    # if not output:
    #     if uid:
    #         success(task_template.render(task=result))
    # else:
    #     if output == "json":
    #         success(json.dumps(result, default=lambda o: o.json, indent=4))
    #     elif output == "yaml":
    #         success(yaml.dump(result, default=lambda o: o.json, indent=4))


    # if result and output:
    #     if output == "json":
    #         result = json.dumps(result, default=lambda o: o.json, indent=4)
    #     elif output == "yaml":
    #         result = yaml.dump(result, default=lambda o: o.json, indent=4)

    # if uid:
    #     success(task_template.render(task=result))
    # else:
    #     pass


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
        success("task is done")


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
