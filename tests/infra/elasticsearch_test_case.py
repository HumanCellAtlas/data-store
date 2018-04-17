import unittest

import os

from dss.config import Config
from tests.es import elasticsearch_delete_index, ElasticsearchServer


class ElasticsearchTestCase(unittest.TestCase):

    server = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        Config.test_index_suffix.prepend(cls.__name__.lower())
        cls.server = ElasticsearchServer()
        os.environ['DSS_ES_PORT'] = str(cls.server.port)

    @classmethod
    def tearDownClass(cls):
        elasticsearch_delete_index('*' + Config.test_index_suffix.value)
        Config.test_index_suffix.restore()
        cls.server.shutdown()
        os.unsetenv('DSS_ES_PORT')
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        Config.test_index_suffix.prepend(self._testMethodName.lower())

    def tearDown(self):
        Config.test_index_suffix.restore()
        super().tearDown()
