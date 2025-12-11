# 0to8_upload

Upload data from the 0to8 backend to Google BigQuery for analytical purposes. [page:50]

## Features

- Automated ETL pipelines from 0to8 API endpoints to BigQuery tables. [page:50]  
- Scheduled runs via GitHub Actions using cron expressions in UTC. [page:50]  
- Centralized configuration for datasets, tables and workflows. [page:50]

## Repository structure

- `.github/workflows/` – CI/CD and cron workflows that orchestrate ETL runs. [page:50]  
- `etl/` – Python scripts that call the API, handle pagination/rate limits and load into BigQuery. [page:50]  
- `definitions/` – schema or table definition files used by the pipelines. [page:50]  
- `workflow_settings.yaml` – shared configuration for workflows (project, dataset, etc.). [page:50]  
- `requirements.txt` – Python dependencies for local runs and GitHub Actions. [page:50]

## Prerequisites

- Google Cloud project with BigQuery enabled and a target dataset created. [page:50]  
- Service account with permissions to insert/update data in the target tables. [page:50]  
- GitHub repository secrets configured for GCP credentials and other sensitive settings. [page:50]

## Setup

1. Clone the repository:
git clone https://github.com/twixfriday/0to8_upload.git
cd 0to8_upload
2. Install dependencies:
pip install -r requirements.txt
3. Configure `workflow_settings.yaml` and required environment variables / secrets. [page:50]

## Running locally

- Use the scripts in `etl/` to test pipelines, for example:  
python etl/<script_name>.py
- Point the scripts at a test BigQuery dataset before running against production. [page:50]

## Automation and scheduling

- Scheduled workflows live in `.github/workflows/*_cron.yml`. [page:50]  
- Each workflow:
- Checks out the repo and sets up Python.  
- Installs dependencies from `requirements.txt`.  
- Runs the appropriate ETL script with configuration from `workflow_settings.yaml`. [page:50]

## Extending

- Add new ETL scripts under `etl/` for additional API endpoints. [page:50]  
- Create or update table definitions in `definitions/`. [page:50]  
- Add a new workflow file in `.github/workflows/` with the desired cron schedule. [page:50]

## License

Specify your license here (e.g. MIT) or link to a `LICENSE` file if present. [web:52]
