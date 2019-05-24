import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa
os.environ["INTEGRATION_TEST"] = 'True'

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    loader = unittest.TestLoader()
    runner.run(loader.discover(os.path.join(pkg_root, 'tests'), pattern="test*api.py"))
