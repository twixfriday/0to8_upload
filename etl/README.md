# ETL Scripts

This folder contains Python scripts that fetch data from REST APIs and load it into Google BigQuery tables. Each script uses `TRUNCATE TABLE` before loading to ensure fresh data snapshots (no duplicates).

## Overview

| Script | Source API | Destination table(s) | Purpose |
| --- | --- | --- | --- |
| `ep_releases_to_bigquery.py` | `/api/admin/promo-releases` | `ep_release`, `ep_timeseries` | Extracts promotional release metadata and timeseries metrics |
| `spotify_timeseries_to_bigquery.py` | `/api/admin/promo-tracks` | `spotify_timeseries`, `spotify_source_streams`, `spotify_streams_by_country` | Breaks down Spotify track data into multiple normalized tables |
| `spotify_tracks_to_bigquery.py` | `/api/admin/promo-tracks` | `spotify_tracks` | Flat snapshot of Spotify track metadata |
| `tiktok_snaps_to_bigquery.py` | `/api/admin/snapshots` | `tiktok_snaps` | TikTok engagement snapshots (views, likes, comments, shares) |
| `promo_exp_to_bigquery.py` | `/api/admin/promo-expenses` | promo expenses | Promo campaign expense data |
| `payment_operations_to_bigquery.py` | `/api/admin/payment-operations` | payment operations | Payment transaction records |

---

## Script Details

### 1. ep_releases_to_bigquery.py

**Purpose**: Fetch promotional music releases and flatten into two tables.

**API endpoint**: `https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin/promo-releases`

**Environment variables**:
- `GCP_PROJECT_ID`: GCP project ID
- `GCP_SERVICE_ACCOUNT_KEY`: JSON service account key
- `API_KEY`: Admin API key (header: `X-Admin-Api-Key`)
- `BQ_EP_SNAP_DATASET_ID` (default: `raw_tiktok`): Dataset for snapshot table
- `BQ_EP_SNAP_TABLE_ID` (default: `ep_release`): Snapshot table name
- `BQ_EP_TS_DATASET_ID` (default: `raw_tiktok`): Dataset for timeseries table
- `BQ_EP_TS_TABLE_ID` (default: `ep_timeseries`): Timeseries table name

**Output tables**:
- `ep_release`: One row per release with aggregated metrics
- `ep_timeseries`: Multiple rows per release, one per date, with daily metrics

**Key fields**: Release title, artist, UPC, Spotify streams/listeners/saves, parse status, last error

---

### 2. spotify_timeseries_to_bigquery.py

**Purpose**: Extract Spotify data from promo tracks and split into three normalized tables for easier querying.

**API endpoint**: `https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin/promo-tracks`

**Environment variables**:
- `GCP_PROJECT_ID`, `GCP_SERVICE_ACCOUNT_KEY`, `API_KEY` (as above, uses `Authorization: Bearer` header)
- `BQ_TS_DATASET_ID` (default: `raw_tiktok`), `BQ_TS_TABLE_ID` (default: `spotify_timeseries`)
- `BQ_SRC_DATASET_ID` (default: `raw_tiktok`), `BQ_SRC_TABLE_ID` (default: `spotify_source_streams`)
- `BQ_CTRY_DATASET_ID` (default: `raw_tiktok`), `BQ_CTRY_TABLE_ID` (default: `spotify_streams_by_country`)

**Output tables**:
- `spotify_timeseries`: Time-indexed metrics (saves, streams, listeners, playlist adds, etc.)
- `spotify_source_streams`: Source breakdown (user, catalog, editorial, network, other, personalized)
- `spotify_streams_by_country`: Geographic breakdown of streams

---

### 3. spotify_tracks_to_bigquery.py

**Purpose**: Flat snapshot of Spotify track metadata (unlike the timeseries script, this is a single denormalized row per track).

**API endpoint**: `https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin/promo-tracks`

**Environment variables**:
- `GCP_PROJECT_ID`, `GCP_SERVICE_ACCOUNT_KEY`, `API_KEY`
- Dataset/table are hardcoded: `raw_tiktok.spotify_tracks`

**Output table**: `spotify_tracks` (one row per track with all metrics in columns)

---

### 4. tiktok_snaps_to_bigquery.py

**Purpose**: Load TikTok engagement snapshots (point-in-time metrics).

**API endpoint**: `https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin/snapshots`

**Environment variables**:
- `GCP_PROJECT_ID`, `GCP_SERVICE_ACCOUNT_KEY`, `API_KEY` (header: `X-Admin-Api-Key`)
- `BQ_SNAPS_DATASET_ID` (default: `raw_tiktok`), `BQ_SNAPS_TABLE_ID` (default: `tiktok_snaps`)

**Output table**: `tiktok_snaps` (views, likes, comments, shares per snapshot date)

---

### 5. promo_exp_to_bigquery.py

**Purpose**: Track promotional campaign expenses.

**API endpoint**: `https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin/promo-expenses`

**Environment variables**:
- `GCP_PROJECT_ID`, `GCP_SERVICE_ACCOUNT_KEY`, `API_KEY`
- `BQ_PROMO_EXP_DATASET_ID` (default: `raw_tiktok`), `BQ_PROMO_EXP_TABLE_ID` (default: `promo_expenses`)

**Output table**: Configurable via env vars, contains expense records

---

### 6. payment_operations_to_bigquery.py

**Purpose**: Load payment transaction history.

**API endpoint**: `https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin/payment-operations`

**Environment variables**:
- `GCP_PROJECT_ID`, `GCP_SERVICE_ACCOUNT_KEY`, `API_KEY`
- `BQ_PAYOPS_DATASET_ID` (default: `raw_tiktok`), `BQ_PAYOPS_TABLE_ID` (default: `payment_operations`)

**Output table**: Configurable via env vars, contains payment records

---

## Running Locally

```bash
# Set environment variables
export GCP_PROJECT_ID="your-project"
export GCP_SERVICE_ACCOUNT_KEY='{"type": "service_account", ...}'
export API_KEY="your-api-key"

# Run a script
python3 etl/spotify_tracks_to_bigquery.py
```

## Data Loading Behavior

All scripts follow the same pattern:

1. **Truncate** the destination table(s) to clear existing data
2. **Paginate** through the API (using `offset` and `limit` parameters)
3. **Flatten/transform** raw API responses into BigQuery-compatible rows
4. **Batch insert** rows in chunks (typically 500 rows per batch)
5. **Log** progress and final row counts

This ensures:
- **No duplicate data** across runs (fresh snapshots only)
- **Consistency** across all ETL scripts
- **Efficient** pagination for large datasets

## Scheduled Execution

Each script is triggered by a GitHub Actions workflow in `.github/workflows/` running on a 3-hour interval starting at different times:

- `promo_exp_cron.yml` → 15:00 GEO (UTC 11:00) every 3 hours
- `payment_operations_cron.yml` → 15:05 GEO (UTC 11:05) every 3 hours
- `spotify_tracks_cron.yml` → 15:15 GEO (UTC 11:15) every 3 hours
- `spotify_timeseries_cron.yml` → 15:20 GEO (UTC 11:20) every 3 hours
- `tiktok_snaps_cron.yml` → 15:30 GEO (UTC 11:30) every 3 hours
- `ep_releases_cron.yml` → 15:40 GEO (UTC 11:40) every 3 hours

See `.github/workflows/README.md` for full cron expression details.

## Monitoring

- Check **GitHub Actions** tab for workflow run history and logs
- Query BigQuery to verify data freshness and row counts
- Review logs in `STDERR` for API errors, truncation failures, or insert errors

## Debugging

Common issues:

| Issue | Cause | Solution |
| --- | --- | --- |
| "BigQuery insert errors" | Malformed rows or schema mismatch | Check API response format and verify table schema |
| "X-Admin-Api-Key" not found | Wrong auth header for some endpoints | Use `X-Admin-Api-Key` for promo endpoints, `Bearer` token for others |
| No rows inserted | API returned empty `data` array | Check `offset`/`limit` pagination, verify API is accessible |
| "TRUNCATE TABLE" fails | Insufficient permissions | Verify service account has `bigquery.tables.update` role |

