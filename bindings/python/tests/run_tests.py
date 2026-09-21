"""Run against the explicitly selected private test package, never the checkout."""

import contextlib
from pathlib import Path
import shutil
import sys
import tempfile
import unittest


@contextlib.contextmanager
def isolate_bytecode_cache(package):
    """Make the selected package import its CURRENT source, or fail by name.

    The package is generated with each source file's own timestamp, so a
    ``__pycache__`` entry beside it can stay valid by size and timestamp while
    the module content has changed, and the interpreter then imports the stale
    bytecode. ``-B`` does not help: it stops writes, not reads, and the
    environment form of the cache prefix is ignored under ``-I``.

    So the cache is removed from the selected package itself -- an owned,
    explicitly named test input, nothing outside it -- which also covers the
    child interpreters the tests spawn, and new bytecode for this process is
    directed to a private prefix so none is recreated beside the source.

    The prefix is owned for the whole body and removed afterwards, on success
    and on failure alike, and the previous prefix is put back. A cleanup
    failure is raised, never swallowed.
    """
    package = Path(package).resolve()
    if not package.is_dir():
        raise SystemExit(f"not a test package directory: {package}")
    for cache in sorted(package.rglob("__pycache__")):
        shutil.rmtree(cache, ignore_errors=True)
    remaining = [str(c) for c in package.rglob("__pycache__")]
    if remaining:
        raise SystemExit("refusing to run with bytecode cached beside the "
                         f"selected package: {', '.join(remaining)}")
    previous = sys.pycache_prefix
    with tempfile.TemporaryDirectory(prefix="moq5-pycache-") as prefix:
        sys.pycache_prefix = prefix
        try:
            yield package
        finally:
            sys.pycache_prefix = previous


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: run_tests.py TEST_PACKAGE_DIRECTORY")
    with isolate_bytecode_cache(sys.argv.pop()) as selected:
        sys.path.insert(0, str(selected))
        suite = unittest.defaultTestLoader.discover(str(Path(__file__).parent),
                                                    pattern="test_*.py")
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
