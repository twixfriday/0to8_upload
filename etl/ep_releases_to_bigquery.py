import os
import json
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

API_BASE_URL = "https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin"
LIMIT = 500  # adjust if needed


def get_bq_client() -> bigquery.Client:
    project_id = os.environ["GCP_PROJECT_ID"]
    sa_key_json = os.environ["GCP_SERVICE_ACCOUNT_KEY"]
    info = json.loads(sa_key_json)
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=project_id, credentials=credentials)


def fetch_page(api_key: str, offset: int):
    url = f"{API_BASE_URL}/promo-releases"
    params = {"limit": LIMIT, "offset": offset}
    headers = {
        "X-Admin-Api-Key": api_key,
        "Accept": "application/json",
    }
    resp = requests.get(url, params=params, headers=headers, timeout=60)
    print("DEBUG status:", resp.status_code, "offset:", offset)
    print("DEBUG body:", resp.text[:300])
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])


def flatten_release_snapshot(rel):
    """
    Snapshot row for ep_release table.
    """
    def to_int(v):
        return int(v) if v not in (None, "") else None

    def to_float(v):
        return float(v) if v not in (None, "") else None

    return {
        "id": str(rel.get("id")),
        "release_title": rel.get("release_title"),
        "artist_name": rel.get("artist_name"),
        "upc": rel.get("upc"),
        "sp_streams_total": to_int(rel.get("sp_streams_total")),
        "sp_listeners_total": to_int(rel.get("sp_listeners_total")),
        "sp_streams_per_listener_total": to_float(rel.get("sp_streams_per_listener_total")),
        "sp_playlist_adds_total": to_int(rel.get("sp_playlist_adds_total")),
        "sp_saves_total": to_int(rel.get("sp_saves_total")),
        "sp_last_day_streams": to_int(rel.get("sp_last_day_streams")),
        "sp_last_day_listeners": to_int(rel.get("sp_last_day_listeners")),
        "sp_last_day_streams_per_listener": to_float(rel.get("sp_last_day_streams_per_listener")),
        "sp_last_day_playlist_adds_total": to_int(rel.get("sp_last_day_playlist_adds_total")),
        "sp_release_date": rel.get("sp_release_date"),
        "sp_total_stream_count": to_int(rel.get("sp_total_stream_count")),
        "sp_updated_at": rel.get("sp_updated_at"),
        "updated_at": rel.get("updated_at"),
        "deleted": rel.get("deleted"),
        "created_at": rel.get("created_at"),
        "last_parse_status": rel.get("last_parse_status"),
        "last_parse_attempt_at": rel.get("last_parse_attempt_at"),
        "last_parse_error": rel.get("last_parse_error"),
    }


def flatten_release_timeseries(rel):
    """
    Timeseries rows for ep_timeseries:
    [{release_id, release_title, date, saves, streams, listeners, playlist_adds, streams_per_listener}, ...]
    """
    release_id = str(rel.get("id"))
    release_title = rel.get("release_title")
    sp_json = rel.get("sp_json") or {}

    def series_for(metric_name):
        metric = sp_json.get(metric_name) or {}
        return metric.get("current_period_timeseries") or []

    saves_ts = {p["x"]: p["y"] for p in series_for("saves")}
    streams_ts = {p["x"]: p["y"] for p in series_for("streams")}
    listeners_ts = {p["x"]: p["y"] for p in series_for("listeners")}
    playlist_adds_ts = {p["x"]: p["y"] for p in series_for("playlist_adds")}
    spl_ts = {p["x"]: p["y"] for p in series_for("streams_per_listener")}

    all_dates = set()
    all_dates.update(saves_ts.keys())
    all_dates.update(streams_ts.keys())
    all_dates.update(listeners_ts.keys())
    all_dates.update(playlist_adds_ts.keys())
    all_dates.update(spl_ts.keys())

    rows = []
    for d in sorted(all_dates):
        rows.append(
            {
                "release_id": release_id,
                "release_title": release_title,
                "date": d,  # "YYYY-MM-DD" -> DATE column
                "saves": int(saves_ts.get(d)) if d in saves_ts else None,
                "streams": int(streams_ts.get(d)) if d in streams_ts else None,
                "listeners": int(listeners_ts.get(d)) if d in listeners_ts else None,
                "playlist_adds": int(playlist_adds_ts.get(d)) if d in playlist_adds_ts else None,
                "streams_per_listener": float(spl_ts.get(d)) if d in spl_ts else None,
            }
        )
    return rows


def main():
    project_id = os.environ["GCP_PROJECT_ID"]

    # snapshot table
    snap_dataset_id = os.environ.get("BQ_EP_SNAP_DATASET_ID", "raw_tiktok")
    snap_table_id = os.environ.get("BQ_EP_SNAP_TABLE_ID", "ep_release")

    # timeseries table
    ts_dataset_id = os.environ.get("BQ_EP_TS_DATASET_ID", "raw_tiktok")
    ts_table_id = os.environ.get("BQ_EP_TS_TABLE_ID", "ep_timeseries")

    api_key = os.environ["API_KEY"]

    client = get_bq_client()
    snap_table_ref = client.dataset(snap_dataset_id).table(snap_table_id)
    ts_table_ref = client.dataset(ts_dataset_id).table(ts_table_id)

    offset = 0
    total_snap_rows = 0
    total_ts_rows = 0

    while True:
        releases = fetch_page(api_key, offset)
        if not releases:
            break

        snap_rows_to_insert = []
        ts_rows_to_insert = []

        for rel in releases:
            # basic sanity: require id
            if rel.get("id") is None:
                continue

            snap_rows_to_insert.append(flatten_release_snapshot(rel))
            ts_rows_to_insert.extend(flatten_release_timeseries(rel))

        if snap_rows_to_insert:
            snap_errors = client.insert_rows_json(snap_table_ref, snap_rows_to_insert)
            if snap_errors:
                raise RuntimeError(f"BigQuery EP snapshot insert errors: {snap_errors}")
            batch_snap = len(snap_rows_to_insert)
            total_snap_rows += batch_snap
            print(f"Inserted EP snapshot batch at offset={offset}, rows={batch_snap}")

        if ts_rows_to_insert:
            ts_errors = client.insert_rows_json(ts_table_ref, ts_rows_to_insert)
            if ts_errors:
                raise RuntimeError(f"BigQuery EP timeseries insert errors: {ts_errors}")
            batch_ts = len(ts_rows_to_insert)
            total_ts_rows += batch_ts
            print(f"Inserted EP timeseries batch at offset={offset}, rows={batch_ts}")

        offset += LIMIT

    print(
        f"Inserted total {total_snap_rows} rows into "
        f"{project_id}.{snap_dataset_id}.{snap_table_id}"
    )
    print(
        f"Inserted total {total_ts_rows} rows into "
        f"{project_id}.{ts_dataset_id}.{ts_table_id}"
    )


if __name__ == "__main__":
    main()
