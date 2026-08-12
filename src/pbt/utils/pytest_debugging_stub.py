"""Stand-in for pytest's built-in ``debugging`` plugin, loaded whenever that
plugin is disabled with ``-p no:debugging``.

PBT disables the real plugin because on Python 3.13+ it does ``import pdb``
at ``pytest_configure`` time, and ``pdb`` in turn does ``import code``. If the
pipeline under test has a top-level package literally named ``code`` (as
Prophecy-generated pipelines do) and its directory is on ``sys.path``, that
import resolves to the pipeline's package instead of the stdlib module and
pytest crashes before any test runs.

Disabling the plugin avoids that crash, but pytest core still expects the
``trace``/``usepdb`` options the debugging plugin normally registers:
``_pytest.unittest.TestCaseFunction.runtest`` and
``_pytest.debugging.maybe_wrap_pytest_function_for_tracing`` both call
``config.getoption(...)``/``config.getvalue(...)`` for them unconditionally,
which raises ``ValueError: no option named ...`` for every test that inherits
``unittest.TestCase`` (i.e. every Prophecy-generated test, since they all
extend ``BaseTestCase``) once the real plugin -- and its option registration
-- is gone. Re-registering the options here as permanent no-ops satisfies
those lookups without ever importing ``pdb``/``code``.
"""


def pytest_addoption(parser):
    parser.addoption(
        "--trace",
        dest="trace",
        action="store_true",
        default=False,
        help="stub option (the real debugging plugin is disabled; see pytest_debugging_stub)",
    )
    parser.addoption(
        "--pdb",
        dest="usepdb",
        action="store_true",
        default=False,
        help="stub option (the real debugging plugin is disabled; see pytest_debugging_stub)",
    )
