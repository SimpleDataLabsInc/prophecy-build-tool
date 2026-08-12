"""Resolve Databricks credentials from env vars or ``~/.databrickscfg``.

Priority (matches the official Databricks CLI conventions so existing user
setup keeps working):

    1. ``DATABRICKS_HOST`` / ``DATABRICKS_TOKEN`` env vars (both must be set).
    2. Service-principal OAuth M2M when ``DATABRICKS_HOST`` plus
       ``DATABRICKS_CLIENT_ID`` / ``DATABRICKS_CLIENT_SECRET`` are set: the
       creds are exchanged for a short-lived bearer token (see
       ``databricks_auth.resolve_databricks_token``).
    3. A profile in a ``.databrickscfg`` INI file:
       - path: ``$DATABRICKS_CONFIG_FILE`` if set, else ``~/.databrickscfg``.
       - profile: ``$DATABRICKS_CONFIG_PROFILE`` if set, else ``DEFAULT``.
"""

from __future__ import annotations

import configparser
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from .databricks_auth import resolve_databricks_token, service_principal_creds_present

DEFAULT_PROFILE = "DEFAULT"
DEFAULT_CONFIG_PATH = "~/.databrickscfg"


@dataclass(frozen=True)
class DatabricksCredentials:
    host: str
    token: str
    source: str  # "env", "service-principal", "databrickscfg:<profile>", or "default"


def _config_path() -> Path:
    return Path(os.environ.get("DATABRICKS_CONFIG_FILE", DEFAULT_CONFIG_PATH)).expanduser()


def _profile_name() -> str:
    return os.environ.get("DATABRICKS_CONFIG_PROFILE", DEFAULT_PROFILE)


def _read_from_cfg(path: Path, profile: str) -> Optional[Tuple[str, str]]:
    if not path.is_file():
        return None

    parser = configparser.ConfigParser()
    try:
        parser.read(path)
    except configparser.Error:
        return None

    # ConfigParser stores ``[DEFAULT]`` as ``parser.defaults()`` and exposes it
    # as a fallback for every section, so handle both code paths explicitly.
    section: Optional[configparser.SectionProxy] = None
    if profile == DEFAULT_PROFILE:
        if parser.defaults():
            section = parser[DEFAULT_PROFILE]
    if section is None and parser.has_section(profile):
        section = parser[profile]

    if section is None:
        return None

    host = (section.get("host") or "").strip()
    token = (section.get("token") or "").strip()
    if not host or not token:
        return None
    return host, token


def get_databricks_credentials(
    env: Optional[dict] = None,
    *,
    default_host: Optional[str] = None,
    default_token: Optional[str] = None,
) -> Optional[DatabricksCredentials]:
    """Return the active Databricks credentials, or ``None`` if nothing is configured.

    ``env`` lets tests pass a pre-built environment; callers at runtime should
    leave it unset so ``os.environ`` is consulted. ``default_host`` /
    ``default_token`` are only used when env, service-principal, and config all
    fall through — this preserves the existing "assume ``test``/``test`` for
    offline tests" behavior without silently masking a misconfigured local
    profile.
    """

    source_env = env if env is not None else os.environ
    env_host = (source_env.get("DATABRICKS_HOST") or "").strip()
    env_token = (source_env.get("DATABRICKS_TOKEN") or "").strip()
    if env_host and env_token:
        return DatabricksCredentials(host=env_host, token=env_token, source="env")

    if env_host and service_principal_creds_present(source_env):
        token = resolve_databricks_token(env_host, default=None, env=source_env)
        if token:
            return DatabricksCredentials(host=env_host, token=token, source="service-principal")

    profile = _profile_name()
    cfg = _read_from_cfg(_config_path(), profile)
    if cfg is not None:
        host, token = cfg
        return DatabricksCredentials(host=host, token=token, source=f"databrickscfg:{profile}")

    if default_host is not None and default_token is not None:
        return DatabricksCredentials(host=default_host, token=default_token, source="default")

    return None


__all__ = ["DatabricksCredentials", "get_databricks_credentials"]
