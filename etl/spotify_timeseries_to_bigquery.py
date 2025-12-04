import os
import json
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

API_BASE_URL = "https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin"
LIMIT = 500  # or the real max

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
    From one promo-track JSON object produce a list of daily rows:
    {isrc, date, saves, streams, listeners, playlist_adds, streams_per_listener}
    """
    isrc = track.get("isrc")
    sp_json = track.get("sp_json") or {}
    data = sp_json.get("data") or {}

    # Each metric has a current_period_timeseries list
    def series_for(metric_name):
        metric = data.get(metric_name) or {}
        return metric.get("current_period_timeseries") or []

    saves_ts = {p["x"]: p["y"] for p in series_for("saves")}
    streams_ts = {p["x"]: p["y"] for p in series_for("streams")}
    listeners_ts = {p["x"]: p["y"] for p in series_for("listeners")}
    playlist_adds_ts = {p["x"]: p["y"] for p in series_for("playlist_adds")}
    spl_ts = {p["x"]: p["y"] for p in series_for("streams_per_listener")}

    # Collect all dates present in any metric
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
                # "x" from API is "YYYY-MM-DD"; table column is DATE so we pass it as is
                "date": d,
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
    dataset_id = os.environ.get("BQ_TS_DATASET_ID", "raw_tiktok")
    table_id = os.environ.get("BQ_TS_TABLE_ID", "spotify_timeseries")
    api_key = os.environ["API_KEY"]

    client = get_bq_client()
    table_ref = client.dataset(dataset_id).table(table_id)

    offset = 0
    total_rows = 0

    while True:
        tracks = fetch_page(api_key, offset)
        if not tracks:
            break

        rows_to_insert = []
        for track in tracks:
            # skip if no ISRC or no sp_json
            if not track.get("isrc") or not track.get("sp_json"):
