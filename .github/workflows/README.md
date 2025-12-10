# GitHub Actions workflows

This folder contains scheduled workflows that run ETL scripts and keep BigQuery tables up to date.

## Workflows overview

| Workflow file | What it runs | BigQuery tables updated | Typical schedule (cron) |
| --- | --- | --- | --- |
| `ep_releases_cron.yml` | `etl/ep_releases_to_bigquery.py` | `ep_release` snapshot table and `ep_timeseries` table (datasets and table names from env vars in the script) | Daily cron (see `on.schedule.cron` in the file) |
| `spotify_timeseries_cron.yml` | `etl/spotify_timeseries_to_bigquery.py` | `spotify_timeseries`, `spotify_source_streams`, `spotify_streams_by_country` | Daily cron |
| `spotify_tracks_cron.yml` | `etl/spotify_tracks_to_bigquery.py` | `spotify_tracks` | Daily cron |
| `tiktok-snaps-cron.yml` | `etl/tiktok_snaps_to_bigquery.py` | `tiktok_snaps` | Daily cron |
| `promo_exp_cron.yml` | `etl/promo_exp_to_bigquery.py` | promo expenses table (dataset/table from env vars) | Daily cron |
| `payment_operations_cron.yml` | `etl/payment_operations_to_bigquery.py` | payment operations table (dataset/table from env vars) | Daily cron |

> Note: Exact run times are defined in each workflow file under the `on.schedule.cron` field. Open the YAML to see the cron expression (UTC) used in this repository.
