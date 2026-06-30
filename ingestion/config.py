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


def get_config():
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise SystemExit(
            f"Missing required env vars: {', '.join(missing)}\n"
            f"Copy .env.example to .env and fill them in."
        )
    return {
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "role":      os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  os.environ.get("SNOWFLAKE_DATABASE", "NYC_TAXI"),
        "raw_schema": os.environ.get("RAW_SCHEMA", "RAW"),
    }
