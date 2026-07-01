#!/usr/bin/env bash
#
# Single entrypoint for every pipeline task. Run from anywhere.
#
#   bash scripts/pipeline.sh <command>
#
# Commands:
#   setup        FIRST-TIME: apply cost guardrails, verify connection, then do a
#                full ingest + dbt build. Idempotent — safe to re-run.
#   guardrails   just (re)apply the Snowflake cost guardrails
#   debug        test the Snowflake connection (dbt debug)
#   ingest       ingest raw data only, skip dbt              [daily run]
#   dbt          build + test the dbt models only, no ingest
#   run          ingest + dbt seed + build + test           [weekly run]
#   teardown     drop ALL Snowflake objects AND delete local artifacts
#
# Env knobs (from .env locally, or secrets/env in CI):
#   MONTHS=2023-01,2023-02   which months to ingest (default 2023-01)
#   MODELS_ENABLED=false     with run/dbt: skip all models
#   SKIP_INSTALL=1           skip pip install (deps cached)
set -euo pipefail
cd "$(dirname "$0")/.."   # repo root — this script lives in scripts/

# Tee ALL output (this script + python + dbt) to a timestamped run log.
mkdir -p logs
RUN_LOG="logs/pipeline_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$RUN_LOG") 2>&1
log_step() { echo ""; echo "=== [$(date '+%Y-%m-%d %H:%M:%S')] $* ==="; }

# --- shared helpers ---------------------------------------------------------
load_env()       { [ -f .env ] && { set -a; . ./.env; set +a; }; return 0; }
ensure_deps() {
  [ "${SKIP_INSTALL:-0}" = "1" ] && return 0
  if command -v uv >/dev/null 2>&1; then
    echo "→ Installing deps with uv (fast)..."
    uv pip install --system -r requirements.txt
  else
    echo "→ Installing deps with pip. TIP: 'pip install uv' makes this ~10-50x faster."
    pip install -r requirements.txt
  fi
}
ensure_profile() {
  export DBT_PROFILES_DIR="$PWD/dbt"
  [ -f dbt/profiles.yml ] || cp dbt/profiles.yml.example dbt/profiles.yml
}
clean_local() {
  # Remove downloaded data, dbt build artifacts, and the generated profile.
  rm -rf data dbt/target dbt/dbt_packages dbt/logs dbt/profiles.yml
  echo "   cleaned: data/, dbt/target, dbt/dbt_packages, dbt/logs, dbt/profiles.yml"
}

load_env

# A relative SNOWFLAKE_PRIVATE_KEY_PATH is portable across machines/CI, but dbt
# runs from dbt/ — so anchor it to the repo root (our cwd) as an absolute path.
case "${SNOWFLAKE_PRIVATE_KEY_PATH:-}" in
  ""|/*|[A-Za-z]:*) : ;;                                   # empty or already absolute
  *) export SNOWFLAKE_PRIVATE_KEY_PATH="$PWD/$SNOWFLAKE_PRIVATE_KEY_PATH" ;;
esac

cmd="${1:-}"
shift 2>/dev/null || true
DBT_ARGS="$*"   # any extra flags (e.g. --full-refresh, --select ...) pass through to dbt build
log_step "pipeline.sh ${cmd:-<none>} ${DBT_ARGS} starting (log: $RUN_LOG)"
case "$cmd" in
  setup)
    ensure_deps; ensure_profile
    log_step "guardrails";  python scripts/run_sql.py scripts/cost_guardrails.sql
    log_step "dbt debug";   ( cd dbt && dbt debug )
    log_step "ingest";      python ingestion/load.py
    log_step "dbt build";   ( cd dbt && dbt seed && dbt build $DBT_ARGS )
    ;;
  guardrails) ensure_deps; log_step "guardrails"; python scripts/run_sql.py scripts/cost_guardrails.sql ;;
  debug)      ensure_deps; ensure_profile; log_step "dbt debug"; ( cd dbt && dbt debug ) ;;
  ingest)     ensure_deps; log_step "ingest"; python ingestion/load.py ;;
  dbt)        ensure_deps; ensure_profile; log_step "dbt build"; ( cd dbt && dbt seed && dbt build $DBT_ARGS ) ;;
  run)
    ensure_deps; ensure_profile
    log_step "dbt debug"; ( cd dbt && dbt debug )
    log_step "ingest";    python ingestion/load.py
    log_step "dbt build"; ( cd dbt && dbt seed && dbt build $DBT_ARGS )
    ;;
  teardown)
    ensure_deps
    log_step "teardown (Snowflake)"; python scripts/run_sql.py scripts/teardown.sql
    log_step "teardown (local)";     clean_local
    ;;
  *)
    echo "usage: bash scripts/pipeline.sh {setup|guardrails|debug|ingest|dbt|run|teardown}"
    exit 1
    ;;
esac
log_step "✅ done: ${cmd}"
