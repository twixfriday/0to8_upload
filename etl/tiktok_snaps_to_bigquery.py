import os
import json
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

BASE_URL = "https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin/snapshots"
LIMIT = 500


def get_bq_client() -> bigquery.Client:
    project_id = os.environ["GCP_PROJECT_ID"]
    sa_key_json = os.environ["GCP_SERVICE_ACCOUNT_KEY"]
    info = json.loads(sa_key_json)
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=project_id, credentials=credentials)


def fetch_page(api_key: str, offset: int):
    params = {
        "limit": LIMIT,
        "offset": offset,
    }
    headers = {
        "X-Admin-Api-Key": api_key,
        "Content-Type": "application/json",
    }
    resp = requests.get(BASE_URL, params=params, headers=headers, timeout=60)
    print("DEBUG status:", resp.status_code, "offset:", offset)
    print("DEBUG body:", resp.text[:300])
    resp.raise_for_status()
    data = resp.json()
    # assuming same shape: {"success": true, "data": [...]}
    return data.get("data", [])


def to_bq_rows(items):
    rows = []
    for x in items:
        rows.append(
            {
                "id": x["id"],
                "promo_expense_id": x.get("promo_expense_id"),
                "views": x.get("views"),
                "likes": x.get("likes"),
                "comments": x.get("comments"),
                "shares": x.get("shares"),
                "snapshot_date": x.get("snapshot_date"),
                "created_at": x.get("created_at"),
            }
        )
    return rows


def main():
    project_id = os.environ["GCP_PROJECT_ID"]
    dataset_id = os.environ.get("BQ_SNAPS_DATASET_ID", "raw_tiktok")
    table_id = os.environ.get("BQ_SNAPS_TABLE_ID", "tiktok_snaps")
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
