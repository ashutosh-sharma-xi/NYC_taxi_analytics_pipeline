"""Run a one-off .sql admin script against Snowflake using env creds.

    python scripts/run_sql.py scripts/cost_guardrails.sql
    python scripts/run_sql.py scripts/teardown.sql

Reads creds from the environment (.env auto-loaded). For admin scripts only —
dbt manages the models; don't run those through here.
"""
import logging
import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import snowflake.connector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("run_sql")


def main(path: str) -> None:
    """Run a one-off admin .sql file against Snowflake.

    - Connects using env creds (key-pair if set, else password).
    - Strips comments, then runs each statement in order.
    - Skips statements that fail harmlessly (e.g. 'warehouse already suspended').
    """
    log.info("executing SQL file: %s", path)
    raw = open(path, encoding="utf-8").read()
    # Strip full-line comments BEFORE splitting so a ';' inside a comment is harmless.
    sql = "\n".join(ln for ln in raw.splitlines() if not ln.strip().startswith("--"))
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    kw = {
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "role":      os.environ.get("SNOWFLAKE_ROLE") or "ACCOUNTADMIN",
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE") or "COMPUTE_WH",
    }
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        # relative path -> anchor to repo root (parent of scripts/)
        if not os.path.isabs(key_path):
            repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            key_path = os.path.join(repo_root, key_path)
        kw["private_key_file"] = key_path
    else:
        kw["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    con = snowflake.connector.connect(**kw)
    cur = con.cursor()
    try:
        for i, stmt in enumerate(statements, 1):
            log.info("[%d/%d] %s", i, len(statements), stmt.splitlines()[0][:80])
            try:
                cur.execute(stmt)
            except snowflake.connector.errors.ProgrammingError as exc:
                # e.g. "cannot suspend, already suspended" in teardown — log & continue
                log.warning("skipped (%s)", exc)
        log.info("SQL file complete: %s", path)
    finally:
        cur.close()
        con.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python scripts/run_sql.py <file.sql>")
    main(sys.argv[1])
