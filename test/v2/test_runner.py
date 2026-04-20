"""Self-tests for the CommandRunner seam.

Verifies:
- ``SubprocessRunner`` actually invokes subprocesses and decodes output.
- ``FakeRunner`` records calls, honors scripted results, and raises scripted errors.

These guards are cheap and pay off when the seam is later relied on by the
hot-path deployment tests.
"""

from __future__ import annotations

import sys

import pytest

from src.pbt.runner import CommandResult, SubprocessRunner
from fakes import FakeRunner

pytestmark = [pytest.mark.unit, pytest.mark.v2]


def test_subprocess_runner_invokes_and_captures() -> None:
    runner = SubprocessRunner()
    result = runner.run([sys.executable, "-c", "print('hello'); import sys; sys.stderr.write('err')"])
    assert result.returncode == 0
    assert result.ok
    assert "hello" in result.stdout
    assert "err" in result.stderr


def test_subprocess_runner_reports_nonzero_return() -> None:
    runner = SubprocessRunner()
    result = runner.run([sys.executable, "-c", "import sys; sys.exit(3)"])
    assert result.returncode == 3
    assert not result.ok


def test_fake_runner_records_calls() -> None:
    fake = FakeRunner()
    fake.run(["mvn", "package", "-DskipTests"], cwd="/tmp", env={"FOO": "1"})
    fake.run(["pip", "install", "requests"])

    assert [c.args for c in fake.calls] == [
        ["mvn", "package", "-DskipTests"],
        ["pip", "install", "requests"],
    ]
    assert fake.calls[0].cwd == "/tmp"
    assert fake.calls[0].env == {"FOO": "1"}
    assert fake.executed("mvn")[0].args[1:] == ["package", "-DskipTests"]


def test_fake_runner_scripts_results_in_order() -> None:
    fake = FakeRunner(
        script=[
            CommandResult(args=["mvn"], returncode=0, stdout="ok"),
            CommandResult(args=["mvn"], returncode=1, stderr="boom"),
        ]
    )
    first = fake.run(["mvn", "package"])
    second = fake.run(["mvn", "package"])

    assert first.returncode == 0
    assert first.stdout == "ok"
    assert second.returncode == 1
    assert second.stderr == "boom"


def test_fake_runner_raises_scripted_exception() -> None:
    fake = FakeRunner(script=[RuntimeError("network down")])
    with pytest.raises(RuntimeError, match="network down"):
        fake.run(["curl", "https://example.com"])


def test_fake_runner_dynamic_response_callable() -> None:
    fake = FakeRunner(
        script=[
            lambda call: CommandResult(args=call.args, returncode=0, stdout=" ".join(call.args)),
        ]
    )
    result = fake.run(["echo", "hi"])
    assert result.stdout == "echo hi"
