## ETL scripts and BigQuery targets

| Script | API endpoint | BigQuery table(s) |
| --- | --- | --- |
| `ep_releases_to_bigquery.py` | `https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin/promo-releases` | Snapshot: ``${GCP_PROJECT_ID}.${BQ_EP_SNAP_DATASET_ID:-raw_tiktok}.${BQ_EP_SNAP_TABLE_ID:-ep_release}``; Timeseries: ``${GCP_PROJECT_ID}.${BQ_EP_TS_DATASET_ID:-raw_tiktok}.${BQ_EP_TS_TABLE_ID:-ep_timeseries}`` |
| `spotify_timeseries_to_bigquery.py` | `https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin/promo-tracks` | Timeseries: ``${GCP_PROJECT_ID}.${BQ_TS_DATASET_ID:-raw_tiktok}.${BQ_TS_TABLE_ID:-spotify_timeseries}``; Source of streams: ``${GCP_PROJECT_ID}.${BQ_SRC_DATASET_ID:-raw_tiktok}.${BQ_SRC_TABLE_ID:-spotify_source_streams}``; Streams by country: ``${GCP_PROJECT_ID}.${BQ_CTRY_DATASET_ID:-raw_tiktok}.${BQ_CTRY_TABLE_ID:-spotify_streams_by_country}`` |
| `spotify_tracks_to_bigquery.py` | `https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin/promo-tracks` | ``${GCP_PROJECT_ID}.raw_tiktok.spotify_tracks`` |
| `tiktok_snaps_to_bigquery.py` | `https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin/snapshots` | ``${GCP_PROJECT_ID}.${BQ_SNAPS_DATASET_ID:-raw_tiktok}.${BQ_SNAPS_TABLE_ID:-tiktok_snaps}`` |
| `promo_exp_to_bigquery.py` | `https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin/promo-expenses` | ``${GCP_PROJECT_ID}.${BQ_PROMO_EXP_DATASET_ID:-raw_tiktok}.${BQ_PROMO_EXP_TABLE_ID:-promo_expenses}`` |
| `payment_operations_to_bigquery.py` | `https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin/payment

