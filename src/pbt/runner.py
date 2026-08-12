"""Command runner seam used to invoke external tools (mvn, pip, spark-submit, pytest, twine).

Product code that needs to spawn subprocesses should take a ``CommandRunner`` via
constructor injection with a default of ``SubprocessRunner()``. Tests can substitute
``FakeRunner`` (in :mod:`test.fakes`) to assert on ``runner.calls`` without really
running anything.

Keeping every subprocess invocation behind this single interface means tests no
longer need to patch ``subprocess`` in many modules.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from typing import Iterable, List, Mapping, Optional, Protocol, Sequence, runtime_checkable


@dataclass(frozen=True)
class CommandResult:
    """Lightweight, serializable analogue of :class:`subprocess.CompletedProcess`.

    Using our own type avoids coupling tests to ``subprocess`` details and lets
    ``FakeRunner`` produce results without constructing a real ``CompletedProcess``.
    """

    args: Sequence[str]
    returncode: int
    stdout: str = ""
    stderr: str = ""

    @property
    def ok(self) -> bool:
        return self.returncode == 0


@dataclass
class CommandCall:
    """Record of a single ``run`` invocation; used by tests for assertions."""

    args: List[str]
    cwd: Optional[str] = None
    env: Optional[Mapping[str, str]] = None
    check: bool = False
    capture_output: bool = True
    shell: bool = False


@runtime_checkable
class CommandRunner(Protocol):
    """Interface every subprocess invocation in the product should flow through."""

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        check: bool = False,
        capture_output: bool = True,
        shell: bool = False,
    ) -> CommandResult: ...


class SubprocessRunner:
    """Default runner that forwards to :func:`subprocess.run`."""

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        check: bool = False,
        capture_output: bool = True,
        shell: bool = False,
    ) -> CommandResult:
        completed = subprocess.run(
            list(args),
            cwd=cwd,
            env=dict(env) if env is not None else None,
            check=check,
            capture_output=capture_output,
            shell=shell,
        )
        stdout = _decode(completed.stdout)
        stderr = _decode(completed.stderr)
        return CommandResult(args=list(args), returncode=completed.returncode, stdout=stdout, stderr=stderr)


def _decode(data) -> str:
    if data is None:
        return ""
    if isinstance(data, bytes):
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return data.decode("utf-8", errors="replace")
    return str(data)


# Single shared default instance; use for modules that cannot take an explicit runner yet.
default_runner: CommandRunner = SubprocessRunner()


__all__ = [
    "CommandCall",
    "CommandResult",
    "CommandRunner",
    "SubprocessRunner",
    "default_runner",
]
