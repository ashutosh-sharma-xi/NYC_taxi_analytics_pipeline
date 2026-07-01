# Analysis queries (Snowflake)

Run these in a Snowsight worksheet **after** loading + building:

```bash
python ingestion/load.py        # land RAW (set MONTHS for more than Jan 2023)
cd dbt && dbt seed && dbt run    # build the marts
```

Adjust the schema prefixes to match your dbt target:
- raw landing  → `NYC_TAXI.RAW`
- marts        → `NYC_TAXI.ANALYTICS_MARTS`  (i.e. `<DBT_SCHEMA>_marts`)

| File | Question |
|---|---|
| `01_volume_by_month.sql`   | Trips, revenue, avg fare/distance per month |
| `02_data_quality_audit.sql`| How many raw rows fail each DQ rule |
| `03_payment_and_tipping.sql`| Payment mix + tipping behaviour |
| `04_top_zones.sql`         | Top pickup zones by trips & revenue |
| `05_hour_of_day.sql`       | Demand pattern by hour of day |
| `06_borough_flow.sql`      | Pickup→dropoff borough flows |
