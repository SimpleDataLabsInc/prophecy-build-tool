"""Unit tests for :mod:`src.pbt.utils.databricks_creds`.

These are pure unit tests: no network, no subprocess, no real Databricks.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.pbt.utils.databricks_creds import get_databricks_credentials

pytestmark = [pytest.mark.unit, pytest.mark.v2, pytest.mark.fast]


@pytest.fixture(autouse=True)
def _clear_databricks_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_CONFIG_FILE",
        "DATABRICKS_CONFIG_PROFILE",
    ):
        monkeypatch.delenv(var, raising=False)


def _write_cfg(path: Path, contents: str) -> None:
    path.write_text(contents, encoding="utf-8")


def test_env_wins_over_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = tmp_path / "databrickscfg"
    _write_cfg(
        cfg,
        "[DEFAULT]\nhost = https://from-cfg\ntoken = cfg-token\n",
    )
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))
    monkeypatch.setenv("DATABRICKS_HOST", "https://from-env")
    monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")

    creds = get_databricks_credentials()
    assert creds is not None
    assert creds.host == "https://from-env"
    assert creds.token == "env-token"
    assert creds.source == "env"


def test_falls_back_to_default_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = tmp_path / "databrickscfg"
    _write_cfg(
        cfg,
        "[DEFAULT]\nhost = https://workspace.example\ntoken = dapi-default\n",
    )
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))

    creds = get_databricks_credentials()
    assert creds is not None
    assert creds.host == "https://workspace.example"
    assert creds.token == "dapi-default"
    assert creds.source == "databrickscfg:DEFAULT"


def test_honors_named_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = tmp_path / "databrickscfg"
    _write_cfg(
        cfg,
        "[DEFAULT]\nhost = https://default\ntoken = default-token\n"
        "\n[prod]\nhost = https://prod.example\ntoken = prod-token\n",
    )
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "prod")

    creds = get_databricks_credentials()
    assert creds is not None
    assert creds.host == "https://prod.example"
    assert creds.token == "prod-token"
    assert creds.source == "databrickscfg:prod"


def test_partial_env_does_not_match_but_config_does(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = tmp_path / "databrickscfg"
    _write_cfg(cfg, "[DEFAULT]\nhost = https://cfg\ntoken = cfg-token\n")
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))
    monkeypatch.setenv("DATABRICKS_HOST", "https://orphaned")
    # No token in env — must fall through to config.

    creds = get_databricks_credentials()
    assert creds is not None
    assert creds.source == "databrickscfg:DEFAULT"
    assert creds.host == "https://cfg"


def test_missing_file_and_no_env_returns_none(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(tmp_path / "does-not-exist"))
    assert get_databricks_credentials() is None


def test_default_fallback_applied_only_when_nothing_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(tmp_path / "missing"))
    creds = get_databricks_credentials(default_host="test", default_token="test")
    assert creds is not None
    assert (creds.host, creds.token, creds.source) == ("test", "test", "default")


def test_blank_values_in_config_are_ignored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = tmp_path / "databrickscfg"
    _write_cfg(cfg, "[DEFAULT]\nhost =\ntoken =\n")
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))

    assert get_databricks_credentials() is None
