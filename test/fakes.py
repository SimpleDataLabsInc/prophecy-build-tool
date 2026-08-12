"""Test doubles used across the suite.

``FakeRunner`` is a recording :class:`~pbt.runner.CommandRunner` that returns
pre-scripted :class:`~pbt.runner.CommandResult` values and captures every call
so tests can assert on command args / cwd / env.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Mapping, Optional, Sequence, Union

from src.pbt.runner import CommandCall, CommandResult

Response = Union[CommandResult, Exception, Callable[[CommandCall], CommandResult]]


@dataclass
class FakeRunner:
    """Recording runner for tests.

    - ``default_result`` is returned for any call that is not otherwise scripted.
    - ``script`` is an ordered queue consulted first: each entry is either a
      ``CommandResult`` to return, an ``Exception`` to raise, or a callable
      ``(CommandCall) -> CommandResult`` for dynamic behavior.
    - ``calls`` records every invocation in order.
    """

    default_result: CommandResult = field(
        default_factory=lambda: CommandResult(args=[], returncode=0, stdout="", stderr="")
    )
    script: List[Response] = field(default_factory=list)
    calls: List[CommandCall] = field(default_factory=list)

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
        call = CommandCall(
            args=list(args),
            cwd=cwd,
            env=dict(env) if env is not None else None,
            check=check,
            capture_output=capture_output,
            shell=shell,
        )
        self.calls.append(call)

        if self.script:
            response = self.script.pop(0)
            if isinstance(response, Exception):
                raise response
            if callable(response):
                return response(call)
            return response
        return CommandResult(
            args=call.args,
            returncode=self.default_result.returncode,
            stdout=self.default_result.stdout,
            stderr=self.default_result.stderr,
        )

    def executed(self, program: str) -> List[CommandCall]:
        """Return all calls whose first argument equals ``program`` (e.g. ``"mvn"``)."""

        return [c for c in self.calls if c.args and c.args[0] == program]

    def reset(self) -> None:
        self.calls.clear()
        self.script.clear()


__all__ = ["FakeRunner"]
