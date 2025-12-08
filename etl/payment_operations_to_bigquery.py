import os
import json
import requests
import re
from google.cloud import bigquery
from google.oauth2 import service_account

API_BASE_URL = "https://tamerlan-0to8-0to8-music-recognition-a469.twc1.net/api/admin"
LIMIT = 500  # page size


def get_bq_client() -> bigquery.Client:
    project_id = os.environ["GCP_PROJECT_ID"]
    sa_key_json = os.environ["GCP_SERVICE_ACCOUNT_KEY"]
    info = json.loads(sa_key_json)
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(project=project_id, credentials=credentials)


def fetch_page(api_key: str, offset: int):
    # adjust path if API uses different name: e.g. "/payment-operations"
    url = f"{API_BASE_URL}/payment-operations"
    params = {
        "limit": LIMIT,
        "offset": offset,
    }
    headers = {
        "X-Admin-Api-Key": api_key,
        "Accept": "application/json",
    }
    resp = requests.get(url, params=params, headers=headers, timeout=60)
    print("DEBUG status:", resp.status_code, "offset:", offset)
    print("DEBUG body:", resp.text[:300])
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success") or not isinstance(data.get("data"), list):
        raise RuntimeError(f"API error or invalid data at offset {offset}: {data}")
    return data["data"]


def to_float(v):
    if v is None or v != v or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    # strip non-numeric characters (e.g. currency symbols, spaces)
    cleaned = re.sub(r"[^0-9.\-]", "", str(v))
    try:
        return float(cleaned) if cleaned != "" else None
    except ValueError:
        return None


def to_bool(v):
    if v is None or v != v:
        return None
    return bool(v)


def to_row(x: dict) -> dict:
    """
    Map one payment_operations JSON object into BigQuery row.
    """
    return {
        "id": str(x.get("id")) if x.get("id") is not None else None,
        "coda_row_id": x.get("coda_row_id"),
        "payee_email": x.get("payee_email"),

        "date_of_request": x.get("date_of_request"),
        "status": x.get("status"),

        "cost": to_float(x.get("cost")),
        "currency": x.get("currency"),

        "payment_cost": to_float(x.get("payment_cost")),
        "payment_currency": x.get("payment_currency"),
        "payment_date": x.get("payment_date"),
        "payment_usd_value": to_float(x.get("payment_usd_value")),

        "account_url": x.get("account_url"),
        "payment_platform": x.get("payment_platform"),
        "unnamed_column": x.get("unnamed_column"),

        "promotional_quantities": x.get("promotional_quantities"),
        "comment": x.get("comment"),

        "telegram_manager_nickname": x.get("telegram_manager_nickname"),
        "telegram_manager_id": x.get("telegram_manager_id"),

        "task_id": x.get("task_id"),
        "profile_id": x.get("profile_id"),
        "profile_name": x.get("profile_name"),

        "currency_conversion_date": x.get("currency_conversion_date"),
        "payment_currency_conversion_date": x.get("payment_currency_conversion_date"),

        "created_at": x.get("created_at"),
        "updated_at": x.get("updated_at"),

        "usd_value": to_float(x.get("usd_value")),
        "deleted": to_bool(x.get("deleted")),
    }


def main():
    project_id = os.environ["GCP_PROJECT_ID"]
    dataset_id = os.environ.get("BQ_PAYOPS_DATASET_ID", "raw_tiktok")
    table_id = os.environ.get("BQ_PAYOPS_TABLE_ID", "payment_operations")

    api_key = os.environ["API_KEY"]

    client = get_bq_client()
    table_ref = client.dataset(dataset_id).table(table_id)

    # *** Overwrite: clear table before inserting this run ***
    truncate_sql = f"truncate table `{project_id}.{dataset_id}.{table_id}`"
    print(f"Running: {truncate_sql}")
    client.query(truncate_sql).result()
    print("Table truncated before load")

    offset = 0
    total_rows = 0

    while True:
        items = fetch_page(api_key, offset)
        if not items:
            break

        rows_to_insert = [to_row(x) for x in items]

        errors = client.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            raise RuntimeError(f"BigQuery payment_operations insert errors: {errors}")

        batch_count = len(rows_to_insert)
        total_rows += batch_count
        print(f"Inserted batch at offset={offset}, rows={batch_count}")

        if len(items) < LIMIT:
            break
        offset += LIMIT

    print(
        f"Inserted total {total_rows} rows into "
        f"{project_id}.{dataset_id}.{table_id}"
    )



if __name__ == "__main__":
    main()
