import os
import json
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

API_BASE_URL = "https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin"
LIMIT = 500 


def get_bq_client() -> bigquery.Client:
    project_id = os.environ["GCP_PROJECT_ID"]
    sa_key_json = os.environ["GCP_SERVICE_ACCOUNT_KEY"]
    info = json.loads(sa_key_json)
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=project_id, credentials=credentials)


def fetch_page(api_key: str, offset: int):
    url = f"{API_BASE_URL}/promo-expenses"
    params = {
        "promo_platform": "TikTok",
        "limit": LIMIT,
        "offset": offset,
    }
    headers = {"X-Admin-Api-Key": api_key}  # must look like this
    resp = requests.get(url, params=params, headers=headers, timeout=60)
    print("DEBUG status:", resp.status_code, "headers sent:", headers)
    print("DEBUG body:", resp.text[:300])
    resp.raise_for_status()
    data = resp.json()
   
    if isinstance(data, list):
        return data
    return data.get("results", [])


def to_bq_rows(items):
    rows = []
    for x in items:
        rows.append(
            {
                "id": x["id"],
                "coda_row_id": x.get("coda_row_id"),
                "telegram_manager_nickname": x.get("telegram_manager_nickname"),
                "telegram_manager_id": x.get("telegram_manager_id"),
                "rate": x.get("rate"),
                "currency": x.get("currency"),
                "promo_link": x.get("promo_link"),
                "promo_date": x.get("promo_date"),
                "parsing_date": x.get("parsing_date"),
                "promo_platform": x.get("promo_platform"),
                "permanent_video_link": x.get("permanent_video_link"),
                "raw_track_title": x.get("raw_track_title"),
                "raw_artist_name": x.get("raw_artist_name"),
                "video_id": x.get("video_id"),
                "profile_id": x.get("profile_id"),
                "profile_name": x.get("profile_name"),
                "spotify_track_title": x.get("spotify_track_title"),
                "spotify_artist_name": x.get("spotify_artist_name"),
                "spotify_isrc": x.get("spotify_isrc"),
                "spotify_upc": x.get("spotify_upc"),
                "views": x.get("views"),
                "likes": x.get("likes"),
                "comments": x.get("comments"),
                "shares": x.get("shares"),
                "last_snapshot_date": x.get("last_snapshot_date"),
                "created_in_coda": x.get("created_in_coda"),
                "duplicate": x.get("duplicate"),
                "original_sound": x.get("original_sound"),
                "created_at": x.get("created_at"),
                "updated_at": x.get("updated_at"),
                "profile_link": x.get("profile_link"),
                "deleted": x.get("deleted"),
                "snapshots_count": x.get("snapshots_count"),
                "sound_url": x.get("sound_url"),
            }
        )
    return rows


def main():
    project_id = os.environ["GCP_PROJECT_ID"]
    dataset_id = os.environ.get("BQ_DATASET_ID", "raw_tiktok")
    table_id = os.environ.get("BQ_TABLE_ID", "promo_exp")
    api_key = os.environ["API_KEY"]

    client = get_bq_client()
    table_ref = client.dataset(dataset_id).table(table_id)

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
