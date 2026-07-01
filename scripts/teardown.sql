-- Run when you're DONE to remove everything and free storage.
--   python scripts/run_sql.py scripts/teardown.sql

USE ROLE ACCOUNTADMIN;

-- Drop the whole project DB (RAW + analytics schemas + all tables/seeds).
DROP DATABASE IF EXISTS NYC_TAXI;

-- Detach + drop the credit guard (optional).
ALTER WAREHOUSE COMPUTE_WH UNSET RESOURCE_MONITOR;
DROP RESOURCE MONITOR IF EXISTS free_tier_guard;

-- A suspended warehouse costs nothing; this just stops it now if running.
-- (Errors harmlessly if it's already suspended.)
ALTER WAREHOUSE COMPUTE_WH SUSPEND;

SHOW DATABASES LIKE 'NYC_TAXI';
