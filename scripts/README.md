# scripts/

Operational scripts — run by you locally or by GitHub Actions. **Nothing here is
"deployed" to a server**: Snowflake is managed SaaS, and the orchestrator
(GitHub Actions) is serverless. "Running it" = executing these scripts.

Everything runs through one entrypoint: **`scripts/pipeline.sh <command>`**.

| File | What it is |
|---|---|
| `pipeline.sh` | **The entrypoint.** Subcommands below. Loads `.env`, sets the dbt profile, dispatches. |
| `cost_guardrails.sql` | Trial protection: `AUTO_SUSPEND=60`, 30-min statement timeout, 20-credit resource monitor. |
| `teardown.sql` | Drops `NYC_TAXI`, removes the guard, suspends the warehouse. |
| `run_sql.py` | Helper `pipeline.sh` uses to run the `.sql` admin files. |

## Usage

```bash
bash scripts/pipeline.sh setup         # FIRST TIME: guardrails + connect + ingest + build
bash scripts/pipeline.sh debug         # test the Snowflake connection
bash scripts/pipeline.sh run           # full pipeline: ingest + dbt build   [weekly]
bash scripts/pipeline.sh ingest        # ingest only, skip dbt               [daily]
bash scripts/pipeline.sh dbt           # dbt build + test only, no ingest
bash scripts/pipeline.sh guardrails    # (re)apply the cost guardrails only
bash scripts/pipeline.sh teardown      # DONE: drop Snowflake objects + wipe local artifacts

# env knobs work with any command:
MONTHS=2023-01,2023-02 bash scripts/pipeline.sh ingest
MODELS_ENABLED=false   bash scripts/pipeline.sh run
```

- **`setup`** is the one-command first run: applies guardrails, verifies the
  connection, loads data, and builds + tests. Idempotent.
- **`teardown`** drops `NYC_TAXI` + the resource monitor in Snowflake **and**
  deletes local artifacts (`data/`, `dbt/target`, `dbt/dbt_packages`,
  `dbt/logs`, `dbt/profiles.yml`) — nothing left behind.

## Do we need to deploy anything?

No. Two ways to run on a schedule, neither needs a hosted server:

- **GitHub Actions (recommended, free):** a workflow checks out the repo and runs
  `scripts/run.sh` on a cron, using repo **Secrets** for the Snowflake creds. The
  only "deploy" step is committing the workflow file and adding the secrets.
- **Airflow (`dags/`):** the realistic-orchestrator demo. It *would* need hosting
  (a VM / Docker / managed Composer), so it's optional — use it to show the
  pattern, not as the free path.

Snowflake (compute + storage) is managed — there's no infrastructure to stand up.
