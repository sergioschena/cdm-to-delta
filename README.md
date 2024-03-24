# CDM Incremental Ingestion

This repo contains a possible implementation of an incremental ingestion of CDM files from the Azure Synapse Link for Dataverse.

This implementation creates delta incremental CSV files from the append-only ones created by the Azure Synapse Link for Dataverse. As those files are multiline CSVs and spark cannot parallelize the parse process of multiline CSVs, being able to ingest only the new appended updates to the entities reduces costs and improves performances.

## Getting Started

There are some example notebooks that are provided:
1. [01_Cdm_Incremental_Copy](./01_Cdm_Incremental_Copy.py), which creates the incremental CSVs from the CDM DataVerse storage location.
1. [02_Cdm_Ingestion](./02_Cdm_Ingestion.py), which performs the incremental ingestion of the CSVs to a parquet (or delta) destination.

## Jobs

### `CdmPartitionIncrementalCopyJob`

This class is responsible for managing the incremental copy of CDM partitions from the ADLS container used by Dataverse Link. 

The incremental copy is based on the metadata stored in the log table, which keeps a pointer to the last location copied for each file.

#### Methods

* `fetch_blob_state_from_log`:
Fetches the latest state of the blobs in the table. It returns the list of `Row`s for the specified `entities`.

* `build_managed_blob_map`:
This method builds a map of the blobs that have already been processed, based on their current state as stored in a database table, managed_partition_blob_log. It takes as input the list of rows returned by the fetch_blob_state_from_log method.

* `select_blob_to_copy`:
This method builds the list of `ManagedBlobLogEntry` to be copied, from the list of `CdmEntity` and the rows that have been extracted from the log table with `fetch_blob_state_from_log`. It selects the blobs that have not yet been processed and returns a list of ManagedBlobLogEntry objects that can be used for copying only new files.

* `copy_incremental_blobs`:
This method is the core of the incremental copy process. Given a list of `ManagedBlobLogEntry` objects, it copies the content of the source blobs to a new set of blobs in the target container. This runs multiple copies in parallel, based on the `max_concurrent_copies` parameter (8 by default).
It returns the list of entries that can be persisted in the log table.

* `persist_log_entries`:
This method persists the log entries of the incremental copy to the log table.

### `CdmBatchIngestionJob`

This is the base class used to ingest CDM entities incremental files to a destination.
It supports three modes for managing writes to the destination:

* Append mode: Data will be appended to the destination.
* Upsert mode: Data will be written to the partition. If a record already exists, the current row's data is merged with the existing row.
* Full mode: This mode overwrites existing data in the destination.

#### Methods

* `copy_cdm_entities_to_destination` and `copy_cdm_entity_to_destination`
Both methods run the ingestion of a single or multiple entities to the destination. 
The first copies all tables in the set and the second performs the copy for a single table.
The methods take the following arguments:
  - `entities`: list of `CdmEntity` to ingest.
  - `mode`: the copy mode, either append, upsert, or full.
  - `update_log`: a flag indicating whether to write to the log table. It defaults to `True` on the multi-entity copy, while it is `False` for the single entity.
  - `enrich_with_option_map`: a flag indicating whether to add description translations, fetched by the `OptionMapReaderJob` to the dataset.

* `persist_log_entries`:
This methods appends the log entries of the current copy process to the log table.

* `read_entity_raw`:
This method reads and parses the CSV files from the incremental CSV location. The list of files to be read comes from the `_get_entity_blobs` method:
1. It reads from the log table of `CdmPartitionIncrementalCopyJob` the list of files that have been copied
1. It reads the files that have been already processed by a previous run from the log table.
1. For the full mode, all the files from the step **1.** are returned, otherwise the diff between **1.** and **2.** is returned.

* `_enrich_with_option_map`:
This method reads the entity localized descriptions using the `OptionMapReaderJob` and appends the descriptions for all the available columns to the dataset.

* `_copy_to_destination`: 
Abstract method that copy the read dataset to the destionation.

* `_build_log_entry`:
Abstract method responsible for builing the log entry.


### CdmToParquetIngestionJob
This is the implementation `CdmBatchIngestionJob` to ingest as parquet files.

It is mandatory to specify `parquet_destination_root_path` in the `Environment` to use this class.

It does not support the `upsert` mode.

### CdmToDeltaIngestionJob
This is the implementation `CdmBatchIngestionJob` to ingest as a delta table in a specified schema.

It is mandatory to specify `delta_destination_schema` in the `Environment` to use this class.

It support all the modes.

### `OptionMapReaderJob`

This job is responsible of reading the localized descriptions from `Microsoft.Athena.TrickleFeedService` folder for a given entity.
It fetches both global and entity specific description and offers also the possibility of patching the dataset before returning it.

#### Methods

* `read_option_map`:
It reads the entity metadata from the JSON file under the TrikleFeedService folder, merges entity and global options in a single dataframe, filter the descriptions of `label_language_code` language and patches the dataframe using `patch_dataframe`. It return a dataframe with the following columns:
  - `EntityName`
  - `IsUserLocalizedLabel`
  - `LocalizedLabel`
  - `LocalizedLabelLanguageCode`
  - `Option`
  - `OptionSetName`


## Model classes

- `Environment`: Container for storing all environment-level configurations (such as Azure Blob Storage credentials and container names, CDM root path, etc). Also includes functions for generating URLs and access tokens used for Azure Blob Storage.
- `SourcePartitionBlob`: Dataclass for storing partition information for blobs being copied.
- `ManagedBlobLogEntry`: Dataclass for logging information on copied blobs including partition information, blob size, process timestamp, and errors encountered during the copy.
- `CdmAttribute`: Class for mapping CDM attributes to pyspark.types. Note that not all CDM attributes have been mapped and the fallback is to map to StringType.
- `CdmEntity`: Class for parsing CDM Entity schema and constructing `StructType` Spark schema. 
- `CdmManifest`: Class for parsing CDM Manifest JSON and collecting all included `CdmEntity` objects. 

The `CdmAttribute` and `CdmEntity` classes are used within `CdmManifest` as attributes of the CDM schema. The `CdmManifest` class is responsible both for parsing the model.json file and for collecting all the entity objects and adding them to the `entities` dict attribute. There is a function (`get_entity`) to return an individual entity object, or a function (`get_entities`) to return the entire `entities` dict.

## Project Support
Please note that this project is provided for your exploration only, and is not formally supported by Databricks with Service Level Agreements (SLAs). It is provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of this project.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.

## Credits

The creation of the incremental CSVs has been replicated from [Dataverse to SQL GitHub repo](https://github.com/Azure/dataverse-to-sql).