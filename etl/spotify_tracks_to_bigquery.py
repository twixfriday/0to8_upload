import os
import json
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

API_BASE_URL = "https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin"
LIMIT = 500  # or 100 if that’s the max for this endpoint


def get_bq_client() -> bigquery.Client:
    project_id = os.environ["GCP_PROJECT_ID"]
    sa_key_json = os.environ["GCP_SERVICE_ACCOUNT_KEY"]
    info = json.loads(sa_key_json)
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=project_id, credentials=credentials)


def fetch_page(api_key: str, offset: int):
    url = f"{API_BASE_URL}/promo-tracks"
    params = {
        "limit": LIMIT,
        "offset": offset,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    resp = requests.get(url, params=params, headers=headers, timeout=60)
    print("DEBUG status:", resp.status_code, "offset:", offset)
    print("DEBUG body:", resp.text[:300])
    resp.raise_for_status()
    data = resp.json()
    # expected shape: { "success": true, "data": [...] }
    return data.get("data", [])


def to_bq_rows(items):
    rows = []
    for x in items:
        rows.append(
            {
                "id": x["id"],
                "track_title": x.get("track_title"),
                "artist_name": x.get("artist_name"),
                "isrc": x.get("isrc"),
                "total_views": x.get("total_views"),
                "total_likes": x.get("total_likes"),
                "total_comments": x.get("total_comments"),
                "total_shares": x.get("total_shares"),
                "sp_streams_total": x.get("sp_streams_total"),
                "sp_listeners_total": x.get("sp_listeners_total"),
                "sp_streams_per_listener_total": x.get("sp_streams_per_listener_total"),
                "sp_playlist_adds_total": x.get("sp_playlist_adds_total"),
                "sp_saves_total": x.get("sp_saves_total"),
                "sp_user_total": x.get("sp_user_total"),
                "sp_network_total": x.get("sp_network_total"),
                "sp_catalog_total": x.get("sp_catalog_total"),
                "sp_other_total": x.get("sp_other_total"),
                "sp_personalized_total": x.get("sp_personalized_total"),
                "sp_editorial_total": x.get("sp_editorial_total"),
                "sp_updated_at": x.get("sp_updated_at"),
                "sp_last_day_streams": x.get("sp_last_day_streams"),
                "sp_last_day_listeners": x.get("sp_last_day_listeners"),
                "sp_last_day_streams_per_listener": x.get("sp_last_day_streams_per_listener"),
                "sp_last_day_playlist_adds_total": x.get("sp_last_day_playlist_adds_total"),
                "sp_release_date": x.get("sp_release_date"),
                "sp_total_stream_count": x.get("sp_total_stream_count"),
                "upc": x.get("upc"),
                "last_parse_status": x.get("last_parse_status"),
                "last_parse_attempt_at": x.get("last_parse_attempt_at"),
                "last_parse_error": x.get("last_parse_error"),
                # explicitly NOT including updated_at or sp_json
            }
        )
    return rows


def main():
    project_id = os.environ["GCP_PROJECT_ID"]
    dataset_id = "raw_tiktok"
    table_id = "spotify_tracks"
    api_key = os.environ["API_KEY"]

    client = get_bq_client()
    table_ref = client.dataset(dataset_id).table(table_id)

    # *** Overwrite: clear table before inserting this run ***
    truncate_sql = f"TRUNCATE TABLE `{project_id}.{dataset_id}.{table_id}`"
    print(f"Running: {truncate_sql}")
    client.query(truncate_sql).result()
    print("Table truncated before load")

    offset = 0
    total_rows = 0

    while True:
        items = fetch_page(api_key, offset)
        if not items:
            break

        rows = to_bq_rows(items)
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

        batch_count = len(rows)
        total_rows += batch_count
        print(f"Inserted batch at offset={offset}, rows={batch_count}")
        offset += LIMIT

    print(f"Inserted total {total_rows} rows into {project_id}.{dataset_id}.{table_id}")


if __name__ == "__main__":
    main()
