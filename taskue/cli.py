import click
from redis import Redis
from taskue.server import TaskueServer
from taskue.runner import TaskueRunner


@click.group()
def server_group():
    pass


@server_group.command()
@click.option("--redis-host", default="localhost", help="redis hostname")
@click.option("--redis-port", default=6379, help="redis port")
def server(redis_host, redis_port):
    """Start a server"""
    redis_connection = Redis(redis_host, redis_port)
    tkue = TaskueServer(redis=redis_connection)
    tkue.start()


@click.group()
def runner_group():
    pass


@runner_group.command()
@click.option("--redis-host", default="localhost", help="redis hostname")
@click.option("--redis-port", default=6379, help="redis port")
@click.option("--tags", default="", help="comma seperated tags, ex: test1,test2")
@click.option("--timeout", default=None, help="runner timeout in seconds")
@click.option(
    "--run_untaged_tasks",
    default=True,
    help="flag to allow runner to run untaged tasks",
)
def runner(redis_host, redis_port, tags, timeout, run_untaged_tasks):
    """Start a runner"""
    tags = tags.strip().split(",")
    redis_connection = Redis(redis_host, redis_port)
    tkue = TaskueRunner(redis=redis_connection, tags=tags)
    tkue.start()


cli = click.CommandCollection(sources=[server_group, runner_group])
