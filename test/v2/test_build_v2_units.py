"""Granular tests for ``build_v2`` that avoid running Maven or the Python toolchain.

``PBTCli.build`` funnels into ``PipelineDeployment.build`` which loops over
pipelines and calls ``PackageBuilderAndUploader.build``. We patch the builder
so we can verify:

- ``--pipelines`` filters by pipeline *name* (not id).
- ``--ignore-build-errors`` swallows a failing build.
- Without it, a failing build triggers ``SystemExit``.

These tests avoid asserting on Rich-rendered stdout, per the refactor plan.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pytest

from src.pbt.pbt_cli import PBTCli
from src.pbt.deployment import pipeline as pipeline_module

pytestmark = [pytest.mark.unit, pytest.mark.v2, pytest.mark.fast]


class _FakeBuilder:
    """Pretend ``PackageBuilderAndUploader`` that returns a pre-set code."""

    def __init__(self, *args, **kwargs) -> None:  # noqa: D401 - match real signature loosely
        self._args = args
        self._kwargs = kwargs

    def build(self, ignore_build_errors: bool = False) -> int:
        return _FakeBuilder.return_code

    return_code = 0  # class attribute so tests can set once and share


def _make_project(tmp_path: Path, pipeline_defs: list[tuple[str, str]]) -> Path:
    """``pipeline_defs`` is a list of ``(pipeline_id, pipeline_name)`` tuples."""

    project = tmp_path / "build-test"
    project.mkdir()
    pipelines_block = "".join(f"  {pid}:\n    name: {pname}\n    language: python\n" for pid, pname in pipeline_defs)
    pipeline_refs = "".join(f"    - {pid}\n" for pid, _ in pipeline_defs)
    (project / "pbt_project.yml").write_text(
        "name: build-test\n"
        "language: python\n"
        "version: 0.0.1\n"
        f"pipelines:\n{pipelines_block}"
        "jobs:\n"
        "  jobs/smoke:\n"
        "    name: smoke\n"
        "    fabricUID: '1'\n"
        "    scheduler:\n      Databricks: {}\n"
        f"    pipelines:\n{pipeline_refs}"
    )
    # Create minimal code folders so load_pipeline_folder returns a non-empty dict.
    for pid, _ in pipeline_defs:
        code = project / pid / "code"
        code.mkdir(parents=True)
        (code / "main.py").write_text("")
    return project


@pytest.fixture
def fake_builder(monkeypatch: pytest.MonkeyPatch) -> type[_FakeBuilder]:
    _FakeBuilder.return_code = 0
    monkeypatch.setattr(pipeline_module, "PackageBuilderAndUploader", _FakeBuilder)
    return _FakeBuilder


def _build_count(monkeypatch: pytest.MonkeyPatch, fake_builder: type[_FakeBuilder]) -> List[int]:
    """Record calls to the fake builder's ``build`` method."""

    calls: list[int] = []
    real_build = fake_builder.build

    def wrapped(self, ignore_build_errors: bool = False) -> int:
        calls.append(1)
        return real_build(self, ignore_build_errors)

    monkeypatch.setattr(fake_builder, "build", wrapped)
    return calls


def test_build_all_pipelines_by_default(tmp_path: Path, fake_builder, monkeypatch) -> None:
    project = _make_project(tmp_path, [("pipelines/alpha", "alpha"), ("pipelines/beta", "beta")])
    calls = _build_count(monkeypatch, fake_builder)

    pbt = PBTCli.from_conf_folder(str(project))
    pbt.build(pipelines="", ignore_build_errors=False, ignore_parse_errors=False, add_pom_python=False)

    assert len(calls) == 2


def test_pipelines_filter_limits_to_named_pipelines(tmp_path: Path, fake_builder, monkeypatch) -> None:
    project = _make_project(tmp_path, [("pipelines/alpha", "alpha"), ("pipelines/beta", "beta")])
    calls = _build_count(monkeypatch, fake_builder)

    pbt = PBTCli.from_conf_folder(str(project))
    pbt.build(pipelines="alpha", ignore_build_errors=False, ignore_parse_errors=False, add_pom_python=False)

    assert len(calls) == 1


def test_build_failure_without_ignore_exits_nonzero(tmp_path: Path, fake_builder) -> None:
    project = _make_project(tmp_path, [("pipelines/alpha", "alpha")])
    fake_builder.return_code = 1

    pbt = PBTCli.from_conf_folder(str(project))
    with pytest.raises(SystemExit) as exc:
        pbt.build(pipelines="", ignore_build_errors=False, ignore_parse_errors=False, add_pom_python=False)
    assert exc.value.code == 1


def test_build_failure_with_ignore_errors_succeeds(tmp_path: Path, fake_builder) -> None:
    project = _make_project(tmp_path, [("pipelines/alpha", "alpha")])
    fake_builder.return_code = 1

    pbt = PBTCli.from_conf_folder(str(project))
    # Should not raise. Swallowing the error is the flag's entire purpose.
    pbt.build(pipelines="", ignore_build_errors=True, ignore_parse_errors=False, add_pom_python=False)


def test_partial_invalid_filter_builds_matching_only(
    tmp_path: Path, fake_builder, monkeypatch
) -> None:
    """Replaces legacy ``test_build_path_pipeline_with_invalid_filter``.

    ``--pipelines alpha,INVALID_PIPELINE_NAME`` builds ``alpha`` and silently
    ignores the unknown name. v2 does not sys.exit on partial-invalid filters
    — the legacy CLI does; that stays covered by ``test/test_build.py``.
    """

    project = _make_project(tmp_path, [("pipelines/alpha", "alpha"), ("pipelines/beta", "beta")])
    calls = _build_count(monkeypatch, fake_builder)

    pbt = PBTCli.from_conf_folder(str(project))
    pbt.build(
        pipelines="alpha,INVALID_PIPELINE_NAME",
        ignore_build_errors=False,
        ignore_parse_errors=False,
        add_pom_python=False,
    )

    assert len(calls) == 1


def test_all_invalid_filter_builds_nothing(tmp_path: Path, fake_builder, monkeypatch) -> None:
    """Replaces legacy ``test_build_path_pipeline_invalid_filter_only``.

    The legacy CLI exits 1 on an all-invalid filter. v2's filter quietly
    yields an empty loop — no builder is invoked and control returns.
    We assert the zero-call invariant to pin that contract down.
    """

    project = _make_project(tmp_path, [("pipelines/alpha", "alpha"), ("pipelines/beta", "beta")])
    calls = _build_count(monkeypatch, fake_builder)

    pbt = PBTCli.from_conf_folder(str(project))
    pbt.build(
        pipelines="NO_SUCH_PIPELINE,ALSO_MISSING",
        ignore_build_errors=False,
        ignore_parse_errors=False,
        add_pom_python=False,
    )

    assert len(calls) == 0
