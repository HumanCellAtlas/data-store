#!/usr/bin/env python
# coding: utf-8

"""
Run selected doctests as a regular unit test.
"""

import doctest
import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite('dss.util.retry'))
    return tests


if __name__ == '__main__':
    unittest.main()
