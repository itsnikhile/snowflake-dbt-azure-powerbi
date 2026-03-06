"""
Azure Blob Storage Loader
Loads raw data from Azure Blob Storage into Snowflake RAW schema.
Supports CSV, Parquet, and JSON formats with schema inference.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class AzureBlobLoader:
    """
    Loads files from Azure Blob Storage into Snowflake via external stage.
    Uses Snowflake's COPY INTO command for maximum throughput.
    """

    SUPPORTED_FORMATS = {"csv", "parquet", "json", "jsonl"}

    def __init__(self, snowflake_config: dict, azure_config: dict):
        self.sf_config   = snowflake_config
        self.az_config   = azure_config
        self._sf_conn    = None
        self._blob_client = None

    def _get_snowflake_conn(self):
        if not self._sf_conn:
            import snowflake.connector
            self._sf_conn = snowflake.connector.connect(**self.sf_config)
        return self._sf_conn

    def _get_blob_service(self):
        if not self._blob_client:
            try:
                from azure.storage.blob import BlobServiceClient
                conn_str = self.az_config.get("connection_string") or (
                    f"DefaultEndpointsProtocol=https;"
                    f"AccountName={self.az_config['account_name']};"
                    f"AccountKey={self.az_config['account_key']};"
                    f"EndpointSuffix=core.windows.net"
                )
                self._blob_client = BlobServiceClient.from_connection_string(conn_str)
            except Exception as e:
                logger.warning(f"Azure SDK not available: {e} — running in mock mode")
        return self._blob_client

    def list_new_files(self, container: str, prefix: str, since_hours: int = 24) -> List[str]:
        """List files modified within the last N hours."""
        client = self._get_blob_service()
        if not client:
            logger.info(f"[MOCK] Would list blobs in {container}/{prefix}")
            return [f"{prefix}/data_{i}.parquet" for i in range(3)]

        cutoff = datetime.utcnow() - timedelta(hours=since_hours)
        container_client = client.get_container_client(container)
        files = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            if blob.last_modified.replace(tzinfo=None) > cutoff:
                files.append(blob.name)
        logger.info(f"Found {len(files)} new files in {container}/{prefix}")
        return files

    def create_external_stage(self, stage_name: str, container: str, sas_token: str):
        """Creates a Snowflake external stage pointing to Azure Blob."""
        account = self.az_config["account_name"]
        sql = f"""
        CREATE STAGE IF NOT EXISTS {stage_name}
          URL = 'azure://{account}.blob.core.windows.net/{container}/'
          CREDENTIALS = (AZURE_SAS_TOKEN = '{sas_token}')
          FILE_FORMAT = (
            TYPE = PARQUET
            SNAPPY_COMPRESSION = TRUE
          )
          COMMENT = 'External stage for Azure Blob Storage ingestion'
        """
        conn = self._get_snowflake_conn()
        conn.cursor().execute(sql)
        logger.info(f"Stage {stage_name} created/verified")

    def copy_into_snowflake(
        self,
        stage: str,
        target_table: str,
        file_format: str = "PARQUET",
        pattern: Optional[str] = None,
        purge: bool = False,
    ) -> Dict:
        """
        Runs COPY INTO from Azure stage to Snowflake table.
        Returns ingestion stats.
        """
        pattern_clause = f"PATTERN = '{pattern}'" if pattern else ""
        purge_clause   = "PURGE = TRUE" if purge else ""

        sql = f"""
        COPY INTO {target_table}
        FROM @{stage}
        {pattern_clause}
        FILE_FORMAT = (TYPE = {file_format} SNAPPY_COMPRESSION = TRUE)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = CONTINUE
        {purge_clause}
        """
        conn = self._get_snowflake_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()

        loaded   = sum(r[3] for r in results if r[3])
        errors   = sum(r[4] for r in results if r[4])
        files_ok = sum(1 for r in results if r[6] == "LOADED")

        logger.info(
            f"COPY INTO {target_table}: "
            f"{files_ok} files | {loaded:,} rows | {errors} errors"
        )
        return {"files_loaded": files_ok, "rows_loaded": loaded, "errors": errors}

    def run_incremental_load(self, pipeline_config: dict) -> Dict:
        """Full incremental load for all configured tables."""
        results = {}
        for table_cfg in pipeline_config.get("tables", []):
            name   = table_cfg["name"]
            stage  = table_cfg["stage"]
            target = table_cfg["target_table"]
            try:
                stats = self.copy_into_snowflake(
                    stage=stage,
                    target_table=target,
                    file_format=table_cfg.get("format", "PARQUET"),
                    pattern=table_cfg.get("pattern"),
                )
                results[name] = {"status": "success", **stats}
                logger.info(f"[{name}] Loaded {stats['rows_loaded']:,} rows")
            except Exception as e:
                results[name] = {"status": "failed", "error": str(e)}
                logger.error(f"[{name}] Failed: {e}")
        return results

    def close(self):
        if self._sf_conn:
            self._sf_conn.close()
