-- Run ONCE as ACCOUNTADMIN to keep the free trial safe.
--   python scripts/run_sql.py scripts/cost_guardrails.sql
-- (or paste into a Snowsight worksheet)

USE ROLE ACCOUNTADMIN;

-- 1) Don't pay for an idle warehouse: suspend after 60s idle, resume on demand,
--    and kill any runaway query after 30 minutes.
ALTER WAREHOUSE COMPUTE_WH SET
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  STATEMENT_TIMEOUT_IN_SECONDS = 1800;

-- 2) Hard credit cap: stop compute well inside the 400-credit trial.
--    Notifies at 75/90%, suspends at 100%, hard-kills at 110%.
CREATE RESOURCE MONITOR IF NOT EXISTS free_tier_guard
  WITH CREDIT_QUOTA = 20
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 75 PERCENT DO NOTIFY
           ON 90 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND
           ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE COMPUTE_WH SET RESOURCE_MONITOR = free_tier_guard;

SHOW RESOURCE MONITORS;
