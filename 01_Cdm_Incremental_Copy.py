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
from cdm_to_delta.jobs import CdmPartitionIncrementalCopyJob

# Credentials
tenant_id = '9f37a392-f0ae-4280-9796-f1864a10effc'
client_id = 'ed573937-9c53-4ed6-b016-929e765443eb'
client_secret = dbutils.secrets.get('oneenvkeys', 'adls-app-key')

# Storage details
account_name = "storage_account_name"
source_container_name = "dataflow-cdm"
target_container_name = "dataflow-cdm"

cdm_root_path = '/Volumes/main/default/vv_dataflow_cdm'
parquet_destination_root_path = '/Volumes/main/default/vv_dataflow_cdm/_parquet_destination'
log_schema = "cdm_test_catalog.default"
table_schema = "cdm_test_catalog.dest_schema"

entities = ["account"]

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

# 2. Extract blobs metadata
copy_job = CdmPartitionIncrementalCopyJob(spark, environment)
partition_blobs_rows = copy_job.fetch_blob_state_from_log(manifest.get_entities().values())
log_entries_to_copy = copy_job.select_blob_to_copy(manifest.get_entities().values(), partition_blobs_rows)
print(f"Found {len(log_entries_to_copy)} blob to copy from")

# COMMAND ----------

# display(partition_blobs_rows)
# print(log_entries_to_copy)

# COMMAND ----------

# 3. Perform the copy operations
log_entries = copy_job.copy_incremental_blobs(log_entries_to_copy)

# COMMAND ----------

log_entries

# COMMAND ----------

# 4. Persist work done in the log table
copy_job.persist_log_entries(log_entries)
