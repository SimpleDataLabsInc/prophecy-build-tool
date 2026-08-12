# Helpers to resolve Databricks credentials.
#
# PBT talks to Databricks through the legacy ``databricks-cli`` ``ApiClient``,
# which only authenticates with a bearer token (a PAT). It has no notion of
# OAuth M2M / service-principal credentials. To let a service principal
# (client_id + client_secret) be used, we lean on the modern ``databricks-sdk``
# unified-auth credential chain to resolve a short-lived bearer token, then hand
# that token to the existing client.

import os
from typing import Dict, Optional

from .constants import DATABRICKS_HOST, DATABRICKS_TOKEN

DATABRICKS_CLIENT_ID = "DATABRICKS_CLIENT_ID"
DATABRICKS_CLIENT_SECRET = "DATABRICKS_CLIENT_SECRET"

_BEARER_PREFIX = "Bearer "


def service_principal_creds_present(env: Optional[Dict[str, str]] = None) -> bool:
    """True when service-principal OAuth M2M creds are available via env vars."""
    source_env = env if env is not None else os.environ
    return bool(source_env.get(DATABRICKS_CLIENT_ID) and source_env.get(DATABRICKS_CLIENT_SECRET))


def resolve_databricks_token(
    host: Optional[str] = None,
    default: Optional[str] = "test",
    env: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Resolve a Databricks bearer token usable by the legacy ``ApiClient``.

    Order of precedence:
      1. ``DATABRICKS_TOKEN`` (a PAT) if set -- preserves existing behaviour.
      2. Service-principal OAuth M2M when ``DATABRICKS_CLIENT_ID`` /
         ``DATABRICKS_CLIENT_SECRET`` are set: the creds are exchanged for a
         short-lived bearer token via ``databricks-sdk``.
      3. ``default`` (``"test"``) so offline unit tests keep working.

    The OAuth exchange is only attempted when service-principal creds are
    explicitly present, so plain runs (no creds) never reach out to the network
    or probe cloud-metadata endpoints.

    ``env`` lets callers (e.g. ``databricks_creds.get_databricks_credentials``)
    pass a pre-built environment instead of relying on ``os.environ``.
    """
    source_env = env if env is not None else os.environ
    token = source_env.get(DATABRICKS_TOKEN)
    if token:
        return token

    if service_principal_creds_present(source_env):
        try:
            from databricks.sdk.core import Config

            cfg = Config(
                host=host or source_env.get(DATABRICKS_HOST),
                client_id=source_env.get(DATABRICKS_CLIENT_ID),
                client_secret=source_env.get(DATABRICKS_CLIENT_SECRET),
                auth_type="oauth-m2m",
            )
            authorization = cfg.authenticate().get("Authorization", "")
            if authorization.startswith(_BEARER_PREFIX):
                return authorization[len(_BEARER_PREFIX) :]
        except Exception:
            # databricks-sdk missing, or creds could not be exchanged -- fall back.
            pass

    return default
