import sys
import unittest
from unittest import mock

import os


pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss.util.email
from tests.infra import testmode
from dss import Replica


class TestEmail(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUp(self):
        pass

    @testmode.standalone
    @mock.patch('dss.util.email.send_email')
    def test_send_email(self, send_email_func):
        send_email_func.return_value = 'Success'
        dss.util.email.send_email('sender', 'receiver', 'subject', 'html', 'txt')
        self.assertEquals(send_email_func('sender', 'receiver', 'subject', 'html', 'txt'), 'Success')

    @testmode.standalone
    @mock.patch('dss.util.email.send_email')
    def test_send_checkout_success_email(self, send_email_func):
        send_email_func.return_value = 'Success'
        dss.util.email.send_checkout_success_email('sender', 'to', 'bucket', 'location', Replica.aws)

        args, kwargs = send_email_func.call_args
        self.assertEqual(args[0], 'sender')
        self.assertEqual(args[1], 'to')
        self.assertEqual(args[2], dss.util.email.SUCCESS_SUBJECT)
        self.assertIn('<html>', args[3])
        self.assertNotIn('<html>', args[4])

    @testmode.standalone
    @mock.patch('dss.util.email.send_email')
    def test_send_checkout_failure_email(self, send_email_func):
        send_email_func.return_value = 'Success'
        dss.util.email.send_checkout_failure_email('sender', 'to', 'cause')

        args, kwargs = send_email_func.call_args
        self.assertEqual(args[0], 'sender')
        self.assertEqual(args[1], 'to')
        self.assertEqual(args[2], dss.util.email.FAILURE_SUBJECT)
        self.assertIn('<html>', args[3])
        self.assertNotIn('<html>', args[4])


if __name__ == '__main__':
    unittest.main()
