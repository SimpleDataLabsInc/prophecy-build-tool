"""``get_python_commands`` uses the CommandRunner seam for binary probing."""

from __future__ import annotations

import pytest

from fakes import FakeRunner
from src.pbt.deployment import get_python_commands
from src.pbt.runner import CommandResult

pytestmark = [pytest.mark.unit, pytest.mark.v2, pytest.mark.fast]


def test_prefers_python3_when_available() -> None:
    fake = FakeRunner(script=[CommandResult(args=[], returncode=0, stdout="Python 3.11")])

    py, pip = get_python_commands("/nonexistent", runner=fake)

    assert (py, pip) == ("python3", "pip3")
    assert fake.executed("python3"), "should have probed python3 first"
    # Only one probe needed when the first succeeds.
    assert len(fake.calls) == 1


def test_falls_back_to_python_when_python3_missing() -> None:
    fake = FakeRunner(
        script=[
            CommandResult(args=[], returncode=127),
            CommandResult(args=[], returncode=0, stdout="Python 2.7"),
        ]
    )

    py, pip = get_python_commands("/nonexistent", runner=fake)

    assert (py, pip) == ("python", "pip")
    assert [c.args[0] for c in fake.calls] == ["python3", "python"]


def test_exits_when_no_python_found() -> None:
    fake = FakeRunner(
        script=[
            CommandResult(args=[], returncode=127),
            CommandResult(args=[], returncode=127),
        ]
    )

    with pytest.raises(SystemExit) as excinfo:
        get_python_commands("/nonexistent", runner=fake)
    assert excinfo.value.code == 1


def test_file_not_found_error_is_handled() -> None:
    fake = FakeRunner(
        script=[
            FileNotFoundError("no python3"),
            CommandResult(args=[], returncode=0),
        ]
    )

    py, pip = get_python_commands("/nonexistent", runner=fake)
    assert (py, pip) == ("python", "pip")
