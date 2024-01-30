# Databricks notebook source
# MAGIC %pip install azure-identity azure-storage-blob

# COMMAND ----------

from databricks.sdk.runtime import *

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from cdm_to_delta.model import (
    Environment,
    CdmManifest,
)
from cdm_to_delta.jobs import CdmToDeltaIngestionJob, CdmToParquetIngestionJob

# Credentials
tenant_id = '9f37a392-f0ae-4280-9796-f1864a10effc'
client_id = 'ed573937-9c53-4ed6-b016-929e765443eb'
client_secret = dbutils.secrets.get('oneenvkeys', 'adls-app-key')

# Storage details
account_name = "plenitudecdmtest"
source_container_name = "dataflow-cdm"
target_container_name = "dataflow-cdm"

cdm_root_path = '/Volumes/main/default/vv_plenitude_dataflow_cdm'
parquet_destination_root_path = '/Volumes/main/default/vv_plenitude_dataflow_cdm/_parquet_destination'
log_schema = "cdm_test_catalog.default"
table_schema = "cdm_test_catalog.dest_schema"

entities = ["egl_intent"]

environment = Environment(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret,
    source_account_name=account_name,
    source_container_name=source_container_name,
    target_account_name=account_name,
    target_container_name=target_container_name,
    cdm_root_path=cdm_root_path,
    log_schema_name=log_schema,
    incremental_csv_container_path=cdm_root_path,
    parquet_destination_root_path=parquet_destination_root_path,
    delta_destination_schema=table_schema
)

# COMMAND ----------

# 1. Read model.json
manifest = CdmManifest(environment, entities)

# COMMAND ----------

# 2. Init job object
ingestion_job = CdmToParquetIngestionJob(spark, environment)
# ingestion_job = CdmToDeltaIngestionJob(spark, environment)

# COMMAND ----------

ingestion_job.copy_cdm_entities_to_destination(entities=manifest.get_entities().values(), update_log=True, mode=CdmToParquetIngestionJob.MODE_APPEND)

# COMMAND ----------

display(ingestion_job.log_entries)
