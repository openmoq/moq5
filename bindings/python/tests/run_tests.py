"""Run against the explicitly selected private test package, never the checkout."""

from pathlib import Path
import sys
import unittest

if len(sys.argv) != 2:
    raise SystemExit("usage: run_tests.py TEST_PACKAGE_DIRECTORY")
sys.path.insert(0, str(Path(sys.argv.pop()).resolve()))
suite = unittest.defaultTestLoader.discover(str(Path(__file__).parent), pattern="test_*.py")
result = unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(not result.wasSuccessful())
