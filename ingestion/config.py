"""Connection config from environment variables (.env locally, secrets in CI).

Nothing is hard-coded — change the env, not the code, to point at a different
Snowflake account. Ingestion lands data in a dedicated RAW schema, kept separate
from the analytics schemas that dbt builds.
"""
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv optional; CI injects real env vars


_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _resolve_key_path(path):
    """A relative key path is anchored to the repo root, so it works no matter
    the working directory (ingestion runs from root, dbt from dbt/) or machine."""
    if path and not os.path.isabs(path):
        return os.path.join(_REPO_ROOT, path)
    return path


def get_config():
    """Read Snowflake settings from environment variables into a dict.

    - Needs ACCOUNT + USER, plus either a key file or a password.
    - Applies sensible defaults for role / warehouse / database / raw schema.
    - Fails fast with a clear message if a required value is missing.
    """
    missing = [k for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER") if not os.environ.get(k)]
    # Auth is either key-pair (preferred; works with MFA-enabled accounts) or password.
    if not (os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH") or os.environ.get("SNOWFLAKE_PASSWORD")):
        missing.append("SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")
    if missing:
        raise SystemExit(
            f"Missing required env vars: {', '.join(missing)}\n"
            f"Copy .env.example to .env and fill them in."
        )
    return {
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ.get("SNOWFLAKE_PASSWORD"),
        "private_key_path": _resolve_key_path(os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")),
        # `or default` (not get's default) so an empty env var — e.g. a missing
        # GitHub secret, which renders as "" — still falls back correctly.
        "role":       os.environ.get("SNOWFLAKE_ROLE") or "ACCOUNTADMIN",
        "warehouse":  os.environ.get("SNOWFLAKE_WAREHOUSE") or "COMPUTE_WH",
        "database":   os.environ.get("SNOWFLAKE_DATABASE") or "NYC_TAXI",
        "raw_schema": os.environ.get("RAW_SCHEMA") or "RAW",
    }


def connect_kwargs(cfg):
    """Turn the config dict into arguments for snowflake.connector.connect().

    - Uses key-pair auth if a private key is set (works with MFA accounts).
    - Otherwise falls back to password auth.
    """
    kw = {
        "account":   cfg["account"],
        "user":      cfg["user"],
        "role":      cfg["role"],
        "warehouse": cfg["warehouse"],
    }
    if cfg.get("private_key_path"):
        kw["private_key_file"] = cfg["private_key_path"]
    else:
        kw["password"] = cfg["password"]
    return kw
