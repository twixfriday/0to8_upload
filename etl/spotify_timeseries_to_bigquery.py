import os
import json
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

API_BASE_URL = "https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin"
LIMIT = 500  # page size for /promo-tracks


def get_bq_client() -> bigquery.Client:
    project_id = os.environ["GCP_PROJECT_ID"]
    sa_key_json = os.environ["GCP_SERVICE_ACCOUNT_KEY"]
    info = json.loads(sa_key_json)
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=project_id, credentials=credentials)


def fetch_page(api_key: str, offset: int):
    url = f"{API_BASE_URL}/promo-tracks"
    params = {"limit": LIMIT, "offset": offset}
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    resp = requests.get(url, params=params, headers=headers, timeout=60)
    print("DEBUG status:", resp.status_code, "offset:", offset)
    print("DEBUG body:", resp.text[:300])
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])


def flatten_sp_json(track):
    """
    From one promo-track JSON object produce rows for spotify_timeseries:
    {isrc, date, saves, streams, listeners, playlist_adds, streams_per_listener}
    """
    isrc = track.get("isrc")
    sp_json = track.get("sp_json") or {}
    data = sp_json.get("data") or {}

    def series_for(metric_name):
        metric = data.get(metric_name) or {}
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
                "isrc": isrc,
                # "x" is "YYYY-MM-DD" -> BigQuery DATE column
                "date": d,
                "saves": int(saves_ts.get(d)) if d in saves_ts else None,
                "streams": int(streams_ts.get(d)) if d in streams_ts else None,
                "listeners": int(listeners_ts.get(d)) if d in listeners_ts else None,
                "playlist_adds": int(playlist_adds_ts.get(d)) if d in playlist_adds_ts else None,
                "streams_per_listener": float(spl_ts.get(d)) if d in spl_ts else None,
            }
        )
    return rows


def flatten_source_of_streams(track):
    """
    Row for spotify_source_streams:
    {isrc, user, other, catalog, network, editorial, personalized}
    """
    isrc = track.get("isrc")
    sp_json = track.get("sp_json") or {}
    sos = sp_json.get("source_of_streams") or {}

    def to_int(v):
        return int(v) if v not in (None, "") else None

    return {
        "isrc": isrc,
        "user": to_int(sos.get("user")),
        "other": to_int(sos.get("other")),
        "catalog": to_int(sos.get("catalog")),
        "network": to_int(sos.get("network")),
        "editorial": to_int(sos.get("editorial")),
        "personalized": to_int(sos.get("personalized")),
    }


def flatten_streams_by_country(track):
    """
    Rows for spotify_streams_by_country:
    {isrc, name, num, localized_country}
    """
    isrc = track.get("isrc")
    sp_json = track.get("sp_json") or {}
    sbc = sp_json.get("streams_by_country") or {}
    geography = sbc.get("geography") or []

    rows = []
    for g in geography:
        num_val = g.get("num")
        rows.append(
            {
                "isrc": isrc,
                "name": g.get("name"),
                "num": int(num_val) if num_val not in (None, "") else None,
                "localized_country": g.get("localized_country"),
            }
        )
    return rows


def main():
    project_id = os.environ["GCP_PROJECT_ID"]

    # timeseries table
    ts_dataset_id = os.environ.get("BQ_TS_DATASET_ID", "raw_tiktok")
    ts_table_id = os.environ.get("BQ_TS_TABLE_ID", "spotify_timeseries")

    # source-of-streams table
    src_dataset_id = os.environ.get("BQ_SRC_DATASET_ID", "raw_tiktok")
    src_table_id = os.environ.get("BQ_SRC_TABLE_ID", "spotify_source_streams")

    # streams-by-country table
    ctry_dataset_id = os.environ.get("BQ_CTRY_DATASET_ID", "raw_tiktok")
    ctry_table_id = os.environ.get("BQ_CTRY_TABLE_ID", "spotify_streams_by_country")

    api_key = os.environ["API_KEY"]

    client = get_bq_client()
    ts_table_ref = client.dataset(ts_dataset_id).table(ts_table_id)
    src_table_ref = client.dataset(src_dataset_id).table(src_table_id)
    ctry_table_ref = client.dataset(ctry_dataset_id).table(ctry_table_id)

    # *** Overwrite: clear tables before inserting this run ***
    truncate_ts_sql = f"TRUNCATE TABLE `{project_id}.{ts_dataset_id}.{ts_table_id}`"
    truncate_src_sql = f"TRUNCATE TABLE `{project_id}.{src_dataset_id}.{src_table_id}`"
    truncate_ctry_sql = f"TRUNCATE TABLE `{project_id}.{ctry_dataset_id}.{ctry_table_id}`"

    print(f"Running: {truncate_ts_sql}")
    client.query(truncate_ts_sql).result()
    print("Timeseries table truncated before load")

    print(f"Running: {truncate_src_sql}")
    client.query(truncate_src_sql).result()
    print("Source-of-streams table truncated before load")

    print(f"Running: {truncate_ctry_sql}")
    client.query(truncate_ctry_sql).result()
    print("Streams-by-country table truncated before load")

    offset = 0
    total_ts_rows = 0
    total_src_rows = 0
    total_ctry_rows = 0

    while True:
        tracks = fetch_page(api_key, offset)
        if not tracks:
            break

        ts_rows_to_insert = []
        src_rows_to_insert = []
        ctry_rows_to_insert = []

        for track in tracks:
            if not track.get("isrc") or not track.get("sp_json"):
                continue

            ts_rows_to_insert.extend(flatten_sp_json(track))
            src_rows_to_insert.append(flatten_source_of_streams(track))
            ctry_rows_to_insert.extend(flatten_streams_by_country(track))

        if ts_rows_to_insert:
            ts_errors = client.insert_rows_json(ts_table_ref, ts_rows_to_insert)
            if ts_errors:
                raise RuntimeError(f"BigQuery TS insert errors: {ts_errors}")
            batch_ts = len(ts_rows_to_insert)
            total_ts_rows += batch_ts
            print(f"Inserted TS batch at offset={offset}, rows={batch_ts}")

        if src_rows_to_insert:
            src_errors = client.insert_rows_json(src_table_ref, src_rows_to_insert)
            if src_errors:
                raise RuntimeError(f"BigQuery SRC insert errors: {src_errors}")
            batch_src = len(src_rows_to_insert)
            total_src_rows += batch_src
            print(f"Inserted SRC batch at offset={offset}, rows={batch_src}")

        if ctry_rows_to_insert:
            ctry_errors = client.insert_rows_json(ctry_table_ref, ctry_rows_to_insert)
            if ctry_errors:
                raise RuntimeError(f"BigQuery CTRY insert errors: {ctry_errors}")
            batch_ctry = len(ctry_rows_to_insert)
            total_ctry_rows += batch_ctry
            print(f"Inserted CTRY batch at offset={offset}, rows={batch_ctry}")

        offset += LIMIT

    print(
        f"Inserted total {total_ts_rows} timeseries rows into "
        f"{project_id}.{ts_dataset_id}.{ts_table_id}"
    )
    print(
        f"Inserted total {total_src_rows} source-of-streams rows into "
        f"{project_id}.{src_dataset_id}.{src_table_id}"
    )
    print(
        f"Inserted total {total_ctry_rows} streams-by-country rows into "
        f"{project_id}.{ctry_dataset_id}.{ctry_table_id}"
    )


if __name__ == "__main__":
    main()
