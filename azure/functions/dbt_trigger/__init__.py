"""
Azure Function — dbt Cloud Job Trigger
HTTP-triggered function that kicks off dbt Cloud jobs and waits for completion.
Used by Azure Data Factory pipeline after Snowflake data load.
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, Optional

import requests
import azure.functions as func

logger = logging.getLogger(__name__)

DBT_CLOUD_URL   = "https://cloud.getdbt.com/api/v2"
DBT_ACCOUNT_ID  = os.environ.get("DBT_ACCOUNT_ID", "")
DBT_API_TOKEN   = os.environ.get("DBT_API_TOKEN", "")
DBT_JOB_ID      = os.environ.get("DBT_JOB_ID", "")

HEADERS = {
    "Authorization": f"Token {DBT_API_TOKEN}",
    "Content-Type":  "application/json",
}


def trigger_dbt_job(job_id: str, cause: str = "ADF trigger") -> Optional[int]:
    """Trigger a dbt Cloud job and return the run ID."""
    url  = f"{DBT_CLOUD_URL}/accounts/{DBT_ACCOUNT_ID}/jobs/{job_id}/run/"
    body = {"cause": cause, "git_branch": "main"}
    resp = requests.post(url, headers=HEADERS, json=body, timeout=30)
    resp.raise_for_status()
    run_id = resp.json()["data"]["id"]
    logger.info(f"dbt Cloud job {job_id} triggered — run_id={run_id}")
    return run_id


def poll_run_status(run_id: int, timeout_seconds: int = 3600) -> Dict:
    """Poll dbt Cloud run until it completes or times out."""
    url     = f"{DBT_CLOUD_URL}/accounts/{DBT_ACCOUNT_ID}/runs/{run_id}/"
    start   = time.time()
    TERMINAL = {"10": "Success", "20": "Error", "30": "Cancelled"}

    while time.time() - start < timeout_seconds:
        resp   = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        run    = resp.json()["data"]
        status = str(run["status"])

        if status in TERMINAL:
            elapsed = round(time.time() - start)
            logger.info(f"Run {run_id} finished: {TERMINAL[status]} in {elapsed}s")
            return {
                "run_id":     run_id,
                "status":     TERMINAL[status],
                "duration_s": elapsed,
                "href":       run.get("href", ""),
            }

        logger.info(f"Run {run_id} status: {run.get('status_humanized')} — waiting...")
        time.sleep(30)

    raise TimeoutError(f"dbt run {run_id} timed out after {timeout_seconds}s")


def main(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("dbt Cloud trigger function invoked")

    try:
        body      = req.get_json() if req.get_body() else {}
        job_id    = body.get("job_id", DBT_JOB_ID)
        cause     = body.get("cause", f"ADF trigger at {datetime.utcnow().isoformat()}")
        wait      = body.get("wait_for_completion", True)

        if not job_id:
            return func.HttpResponse(
                json.dumps({"error": "job_id is required"}),
                status_code=400,
                mimetype="application/json",
            )

        run_id = trigger_dbt_job(job_id, cause)

        if not wait:
            return func.HttpResponse(
                json.dumps({"run_id": run_id, "status": "triggered"}),
                status_code=202,
                mimetype="application/json",
            )

        result = poll_run_status(run_id)
        status_code = 200 if result["status"] == "Success" else 500

        return func.HttpResponse(
            json.dumps(result),
            status_code=status_code,
            mimetype="application/json",
        )

    except Exception as e:
        logger.error(f"Function failed: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json",
        )
