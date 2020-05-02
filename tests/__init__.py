from gevent import monkey
monkey.patch_all()

import unittest
from redis import Redis
from taskue import Taskue
from taskue.runner import TaskueRunner
import gevent

class BaseTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis = Redis()
        self.cl = Taskue(self.redis)
        self.runners = {}

    def setUp(self):
        self.redis.flushall()
        for i in range(3):
            runner_name = "runner-%s" % i
            runner = TaskueRunner(self.redis, name=runner_name, queues=[runner_name])
            self.runners[runner_name] = gevent.Greenlet.spawn(runner.start)

    def tearDown(self):
        for runner in self.runners:
            self.runners[runner].kill()

    def kill_runner(self, name):
        self.runners[name].kill()
