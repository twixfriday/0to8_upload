# GitHub Actions workflows

This folder contains scheduled workflows that run ETL scripts and keep BigQuery tables up to date.

## Workflows overview

| Workflow file | What it runs | BigQuery tables updated | Schedule (UTC cron) | Local time (GEO UTC+4) |
| --- | --- | --- | --- | --- |
| `promo_exp_cron.yml` | `etl/promo_exp_to_bigquery.py` | promo expenses table (dataset/table from env vars) | `0 11/3 * * *` | 15:00, 18:00, 21:00, … every 3 hours |
| `payment_operations_cron.yml` | `etl/payment_operations_to_bigquery.py` | payment operations table (dataset/table from env vars) | `5 11/3 * * *` | 15:05, 18:05, 21:05, … every 3 hours |
| `spotify_tracks_cron.yml` | `etl/spotify_tracks_to_bigquery.py` | `spotify_tracks` | `15 11/3 * * *` | 15:15, 18:15, 21:15, … every 3 hours |
| `spotify_timeseries_cron.yml` | `etl/spotify_timeseries_to_bigquery.py` | `spotify_timeseries`, `spotify_source_streams`, `spotify_streams_by_country` | `20 11/3 * * *` | 15:20, 18:20, 21:20, … every 3 hours |
| `tiktok-snaps-cron.yml` | `etl/tiktok_snaps_to_bigquery.py` | `tiktok_snaps` | `30 11/3 * * *` | 15:30, 18:30, 21:30, … every 3 hours |
| `ep_releases_cron.yml` | `etl/ep_releases_to_bigquery.py` | `ep_release` snapshot table and `ep_timeseries` table (datasets/tables from env vars) | `40 11/3 * * *` | 15:40, 18:40, 21:40, … every 3 hours |

## Cron expression details

All workflows use the same pattern: **start at specific time (UTC), then run every 3 hours**.

Cron format: `minute hour/interval * * *`

- `0 11/3 * * *` → Runs at 11:00, 14:00, 17:00, 20:00, 23:00 UTC (then 03:00 next day)
- `5 11/3 * * *` → Runs at 11:05, 14:05, 17:05, 20:05, 23:05 UTC
- etc.

> **Note**: GitHub Actions uses UTC time. Georgia timezone is UTC+4, so subtract 4 hours from local time to get UTC.
> For example: 15:00 GEO = 11:00 UTC, 15:05 GEO = 11:05 UTC, etc.

## Truncate behavior

Each ETL script in this repo includes a `TRUNCATE TABLE` step before loading data. This ensures:
- **No duplicate data** across runs
- **Fresh snapshots** of API data on each execution
- **Clean state** between scheduled runs

To disable or modify this behavior, edit the respective Python script in the `etl/` folder.
