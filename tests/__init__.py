import unittest
from redis import Redis
from taskue import Taskue

class BaseTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cl = Taskue(Redis())
