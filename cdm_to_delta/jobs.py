import concurrent.futures
from datetime import datetime
from dataclasses import asdict
from typing import List, Dict, Tuple
import os

from azure.storage.blob import BlobClient, BlobProperties

from pyspark.sql import Window, SparkSession, Row, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pandas as pd

from .adls import copy_range_from_url
from .model import Environment, CdmEntity, ManagedBlobLogEntry, SourcePartitionBlob


class CdmPartitionIncrementalCopyJob(object):
    """Job responsible for managing the incremental copy of CDM partitions from the ADLS
    container used by Dataverse Link.

    The job uses a log table as a checkpoint location.
    """

    log_table_schema = T.StructType(
        [
            T.StructField("entity_name", T.StringType()),
            T.StructField("partition_name", T.StringType()),
            T.StructField("source_blob_url", T.StringType()),
            T.StructField("target_blob_url", T.StringType()),
            T.StructField("offset_start", T.LongType()),
            T.StructField("offset_end", T.LongType()),
            T.StructField("blob_length", T.LongType()),
            T.StructField("process_ts", T.TimestampType()),
            T.StructField("error", T.StringType()),
        ]
    )
    """Schema of the log table"""

    log_table_name = "managed_partition_blob_log"
    """Name of the log table"""

    def __init__(self, spark: SparkSession, environment: Environment) -> None:
        """
        :param spark: :class:`SparkSession`
          Reference to the SparkSession
        :param environment: :class:`Environment`
          The class containing all the relevant environment-related variables
        """
        self.spark = spark
        self.environment = environment
        self.partition_blob_log_table_name = f"{self.environment.log_schema_name}.{self.log_table_name}"
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.environment.log_schema_name}")

    def fetch_blob_state_from_log(self, entities: List[CdmEntity] = []) -> List[Row]:
        """Read the latest state for the known blobs for the specified entities.
        As the log table is written in append mode, the latest state is read based on the most
        recent `process_ts`.

        If the log table does not exists, it is created by this method.

        :param entities: List[:class:`CdmEntity`] (optional)
          The list of :class:`CdmEntity` for which fetch the latest blob state.
        :return: List[:class:`Row`]
          The collected result of the query, as a list of :class:`Row`.
        """
        entity_names = [e.name for e in entities]

        # Create the log table if not exists.
        try:
            partition_blob_log_df = self.spark.table(self.partition_blob_log_table_name)
        except:
            print(
                f"CdmPartitionIncrementalCopyJob - {self.partition_blob_log_table_name} does not exist. "
                f"Creating it..."
            )
            partition_blob_log_df = self.spark.createDataFrame([], self.log_table_schema)
            partition_blob_log_df.write.saveAsTable(self.partition_blob_log_table_name)

        print(
            f"CdmPartitionIncrementalCopyJob - Reading latest blob state from "
            f"{self.partition_blob_log_table_name}..."
        )

        w = Window.partitionBy("source_blob_url").orderBy(
            F.col("process_ts").desc(),
        )

        partition_blobs_rows = (
            partition_blob_log_df.select(
                "entity_name",
                "partition_name",
                "source_blob_url",
                "offset_end",
                "process_ts",
            )
            .where(F.lit(not entity_names) | F.col("entity_name").isin(entity_names))
            .where(F.col("error").isNull())
            .withColumn("row_number", F.row_number().over(w))
            .orderBy("source_blob_url", "row_number")
            .where("row_number = 1")
            .drop("row_number", "process_ts")
        ).collect()

        return partition_blobs_rows

    def select_blob_to_copy(
        self, entities: List[CdmEntity], partition_blob_rows: List[Row]
    ) -> List[ManagedBlobLogEntry]:
        """Build the list of details for the blobs that need to be copied:
        1. Extract the metadata from the blob state rows
        2. Traverse the source container of the Dataverse link to get the blob metadata
        3. If the traversed blob is new or its size is bigger than the latest copied offset, a new entry
        containing the metadata for the copy operation is created (source url, target url, offset and length)

        The steps 2. and 3. are repeated for each entity.

        :param entities: List[:class:`CdmEntity`]
          The list of :class:`CdmEntity` you want to copy.
        :param partition_blob_rows: List[:class:`Row`]
          The list of latest blob state returned by the `fetch_blob_state_from_log` method.
        :return: List[:class:`ManagedBlobLogEntry`]
          The list of metadata for the copy operations as :class:`ManagedBlobLogEntry`.
        """

        # Extract the metadata from the blob rows
        managed_blob_state: Dict[str, Dict[str, SourcePartitionBlob]] = dict()
        for r in partition_blob_rows:
            if r.entity_name not in managed_blob_state:
                managed_blob_state[r.entity_name] = dict()
            managed_blob_state[r.entity_name][r.partition_name] = SourcePartitionBlob(
                r.entity_name, r.partition_name, r.source_blob_url, r.offset_end
            )

        source_container_url = self.environment.source_container_client.url
        target_root_url = self.environment.target_path_url

        process_ts = datetime.now()
        log_entries_to_copy: List[ManagedBlobLogEntry] = list()

        for entity in entities:
            # Traverse the entity folder in the source container
            for b in self.environment.source_container_client.walk_blobs(
                name_starts_with=f"{entity.name}/", delimiter="/"
            ):
                if not isinstance(b, BlobProperties):
                    continue

                # Source file info
                source_uri = b.name
                source_url = f"{source_container_url}/{source_uri}"
                source_file = os.path.basename(source_uri)
                partition_name = os.path.splitext(source_file)[0]

                # Compute offset and length
                partition_blob = managed_blob_state.get(entity.name, {}).get(
                    partition_name,
                    SourcePartitionBlob(entity.name, partition_name, source_url, 0),
                )
                offset = partition_blob.offset
                current_blob_size = b.size
                new_blob_size = current_blob_size - offset

                if new_blob_size <= 0:
                    print(
                        f"CdmPartitionIncrementalCopyJob - Entity: {entity.name} - "
                        f"Content unchanged for {source_url}. Skipping copy"
                    )
                    continue

                # Target file info
                target_file_name = f"{partition_name}_{datetime.strftime(process_ts, '%Y%m%d%H%M%S')}.csv"
                target_url = f"{target_root_url}/{entity.name}/{target_file_name}"

                log_entry = ManagedBlobLogEntry.from_source_blob(
                    partition_blob, target_url, new_blob_size, process_ts, None
                )
                log_entries_to_copy.append(log_entry)

        return log_entries_to_copy

    def copy_incremental_blobs(
        self, blobs_to_copy: List[ManagedBlobLogEntry], max_concurrent_copies: int = 8
    ) -> List[ManagedBlobLogEntry]:
        """Copy the blobs as per the metadata list passed as argument.
        The copy leverages low-level Blob Storage's APIs to implement a server-side copy of the exact portion of
        the source files.
        The copies are run in parallel threads; at most `max_concurrent_copies` threads are running in parallel.

        :param blobs_to_copy: List[:class:`ManagedBlobLogEntry`]
          The list of metadata for the copy operations
        :param max_concurrent_copies: int (optional)
          The number of parallel copies. It defaults to 8
        :return: List[:class:`ManagedBlobLogEntry`]
          The list of the copied metadata. Entry error is set in case something went wrong during the copy.
        """

        def copy_partition_blob(
            entry: ManagedBlobLogEntry,
            environment: Environment,
            source_token: str,
            index: int,
            total: int,
        ):
            print(
                f"CdmPartitionIncrementalCopyJob - {index+1}/{total} Entity: {entry.entity_name} - "
                f"Copying {entry.blob_length} bytes from {entry.source_blob_url} to {entry.target_blob_url} -"
                f" Range: {entry.offset_start}-{entry.offset_end}"
            )

            target_blob_client = BlobClient.from_blob_url(entry.target_blob_url, environment.credential)
            copy_range_from_url(
                source_url=entry.source_blob_url,
                target_blob_client=target_blob_client,
                source_authorization=f"Bearer {source_token}",
                offset=entry.offset_start,
                length=entry.blob_length,
            )

        log_entries: List[ManagedBlobLogEntry] = list()
        source_token = self.environment.get_storage_token()
        total = len(blobs_to_copy)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent_copies) as executor:
            future_to_blob_entry = {
                executor.submit(
                    copy_partition_blob,
                    blob,
                    self.environment,
                    source_token.token,
                    idx,
                    total,
                ): blob
                for idx, blob in enumerate(blobs_to_copy)
            }
            for future in concurrent.futures.as_completed(future_to_blob_entry):
                entry = future_to_blob_entry[future]
                try:
                    pass
                except Exception as exc:
                    entry.error = repr(exc)
                log_entries.append(entry)

        return log_entries

    def persist_log_entries(self, log_entries: List[ManagedBlobLogEntry]) -> None:
        """Append the log entries in the log table.
        If the list is empty, it return immediately.

        :param log_entries: List[:class:`ManagedBlobLogEntry`]
          The list of copy metadata to be saved in the log table.
        """
        if not log_entries:
            return

        log_df = self.spark.createDataFrame((Row(**asdict(le)) for le in log_entries), self.log_table_schema)
        log_df.write.saveAsTable(self.partition_blob_log_table_name, mode="append")


class CdmBatchIngestionJob(object):
    """Job responsible for ingesting the incremental files to the target.
    The job uses a log table to checkpoint the files that have been already ingested.

    This is an abstract class and must not be used directly.
    When extending it, you will need to implement:
    - `_copy_to_destination`
    - `_build_log_entry`.
    """

    cdm_timestamp_format = "MM/dd/yyyy hh:mm:ss aa"
    """Timestamp format for CDM `SinkCreatedOn` and `SinkModifiedOn` fields."""

    log_table_schema = T.StructType(
        [
            T.StructField("entity_name", T.StringType()),
            T.StructField("destination", T.StringType()),
            T.StructField("destination_type", T.StringType()),
            T.StructField("mode", T.StringType()),
            T.StructField("processing_dt", T.TimestampType()),
            T.StructField("copy_start_dt", T.TimestampType()),
            T.StructField("copy_end_dt", T.TimestampType()),
            T.StructField("copy_duration_seconds", T.DoubleType()),
            T.StructField("source_blobs", T.ArrayType(T.StringType())),
        ]
    )
    """Schema of the log table"""

    log_table_name = "cdm_to_raw_ingestion_log"
    """Name of the log table"""

    MODE_APPEND = "append"
    """Key for the ingestion append mode"""

    MODE_UPSERT = "upsert"
    """Key for the ingestion upsert mode"""

    MODE_FULL = "full"
    """Key for the ingestion full mode"""

    MODES = [MODE_APPEND, MODE_FULL, MODE_UPSERT]
    """List of the supported ingestion modes"""

    def __init__(self, spark: SparkSession, environment: Environment) -> None:
        """
        :param spark: :class:`SparkSession`
          Reference to the SparkSession
        :param environment: :class:`Environment`
          The class containing all the relevant environment-related variables
        """
        self.spark = spark
        self.environment = environment
        self.full_log_table_name = f"{self.environment.log_schema_name}.{self.log_table_name}"
        self.log_entries: List[Row] = list()
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.environment.log_schema_name}")

    def copy_cdm_entities_to_destination(
        self,
        *,
        entities: List[CdmEntity],
        mode: str = MODE_APPEND,
        update_log: bool = True,
        enrich_with_option_map: bool = False,
    ):
        """Ingest the listed entities to the destination.
        It loops on each entity and calls `copy_cdm_entity_to_destination`.
        If specified, the log is updated for each entity individually.

        :param entities: List[:class:`CdmEntity`]
          List of entities to ingest
        :param mode: str (optional)
          Ingestion mode. It defaults to `MODE_APPEND`
        :param update_log: bool (optional)
          Whether to update the log or not. It defaults to True
        :param enrich_with_option_map: bool (optional)
          Whether to enrich the raw data with string-mapped options or not. It defaults to False.
        """
        for entity in entities:
            self.copy_cdm_entity_to_destination(
                entity=entity,
                mode=mode,
                update_log=update_log,
                enrich_with_option_map=enrich_with_option_map,
            )

    def copy_cdm_entity_to_destination(
        self,
        *,
        entity: CdmEntity,
        mode: str = MODE_APPEND,
        update_log: bool = False,
        enrich_with_option_map: bool = True,
    ) -> None:
        """Ingest a single entity to the destination.
        The ingestion process does the following:
        1. Read the incremental files, based on the ingestion mode and the status of the checkpoint tables
        2. (Optional) Enrich the raw data with the string-mapped options
        3. Save the data to the destination
        4. Build the log entry
        5. (Optional) Persist the log entries

        :param entity: :class:`CdmEntity`
          Entity to ingest
        :param mode: str (optional)
          Ingestion mode. It defaults to `MODE_APPEND`
        :param update_log: bool (optional)
          Whether to update the log or not. It defaults to True
        :param enrich_with_option_map: bool (optional)
          Whether to enrich the raw data with string-mapped options or not. It defaults to False.
        """
        if update_log:
            self.log_entries = list()

        processing_datetime = datetime.now()

        entity_df, blobs = self.read_entity_raw(entity=entity, mode=mode)

        if entity_df.isEmpty():
            print(f"{entity.name} - copy_cdm_entity_to_destination - Nothing to do")
            return

        if enrich_with_option_map:
            entity_df = self._enrich_with_option_map(entity_df, entity.name)

        copy_start_time = datetime.now()

        destination = self._copy_to_destination(entity_df, entity, mode, processing_datetime)

        copy_end_time = datetime.now()
        copy_duration = (copy_end_time - copy_start_time).total_seconds()
        print(f"{entity.name} - copy_cdm_entity_to_destination - Copy took {copy_duration:0.3f}s")

        # Build log entry
        self.log_entries.append(
            self._build_log_entry(
                entity.name,
                destination,
                mode,
                processing_datetime,
                copy_start_time,
                copy_end_time,
                copy_duration,
                blobs,
            )
        )

        # Commit to log tables if requested
        if update_log:
            self.persist_log_entries()

    def read_entity_raw(
        self,
        *,
        entity: CdmEntity,
        mode: str,
    ) -> Tuple[DataFrame, List[str]]:
        """Read the entity's incremental files, applying the correct schema at read, as specified by the `model.json`
        manifest file.

        The read process does the following:
        1. Obtain the list of files to read, based on the ingestion mode and the status of the checkpoint tables
        2. Map the files location, from the `https` path to the Volume path
        3. Append additional metadata (file path and file modification time) to the data.

        :param entity: :class:`CdmEntity`
          Entity to read
        :param mode: str
          Ingestion mode. It defaults to `MODE_APPEND`
        :return: Tuple[:class:`DataFrame`, List[str]]
          A tuple containing the spark :class:`DataFrame` and the list of incremental files read
        """
        blobs = self._get_entity_blobs(entity.name, mode)
        paths = [self.environment.blob_url_to_path(b) for b in blobs]
        if not paths:
            schema = (
                T.StructType(entity.schema.fields)
                .add(T.StructField("_partition_modification_time", T.TimestampType()))
                .add(T.StructField("_file_path", T.StringType()))
            )
            df = self.spark.createDataFrame([], schema)
        else:
            ts_filter = (
                (F.col("SinkCreatedOn").isNull())
                | (F.to_date(F.col("SinkCreatedOn")) <= F.lit(pd.Timestamp.max))
            ) & (
                (F.col("SinkModifiedOn").isNull())
                | (F.to_date(F.col("SinkModifiedOn")) <= F.lit(pd.Timestamp.max))
            )
            df = (
                self.spark.read.schema(entity.schema)
                .csv(
                    path=paths,
                    multiLine=True,
                    escape='"',
                    header=False,
                    sep=",",
                    timestampFormat=self.cdm_timestamp_format,
                )
                .where(ts_filter)
                .withColumn(
                    "_partition_modification_time",
                    F.col("_metadata.file_modification_time"),
                )
                .withColumn(
                    "_file_path",
                    F.col("_metadata.file_path"),
                )
            )
        return df, blobs

    def persist_log_entries(self) -> None:
        """Persist the log entries in the log table.
        If the list is empty, it return immediately.
        """
        if not self.log_entries:
            return
        log_df = self.spark.createDataFrame(self.log_entries, self.log_table_schema)
        log_df.write.saveAsTable(self.full_log_table_name, mode="append")

    def _get_entity_blobs(self, entity_name: str, mode: str) -> List[str]:
        """Extract the list of entity's incremental files based on the ingestion mode and the status of
        the checkpoint log tables, doing the following:
        1. Fetch the list of incremental files successfully generated
        2. Fetch the list of incremental files successfully ingested in the destination
        3. Exclude the files already ingested.

        The steps 2. and 3. are not performed if the ingestion mode is `MODE_FULL`.

        :param entity_name: str
          The entity name
        :param mode: str
          The ingestion mode
        :return: List[str]
          The list of incremental files to read
        """
        assert mode in self.MODES, f"Mode {mode} is not supported. Choose one of {','.join(self.MODES)}"

        copy_job = CdmPartitionIncrementalCopyJob(self.spark, self.environment)

        print(f"{entity_name} - _get_entity_blobs - Fetching available blobs...")

        # full
        blobs_df = (
            self.spark.table(copy_job.partition_blob_log_table_name)
            .where(f"entity_name = '{entity_name}' AND error IS NULL")
            .select("target_blob_url")
        )

        if self.MODE_FULL != mode:
            # Read blobs already processed
            try:
                log_table_df = self.spark.table(self.full_log_table_name)
            except:
                print(
                    f"{self.__class__.__name__} - {self.full_log_table_name} does not exist. Creating it..."
                )
                log_table_df = self.spark.createDataFrame([], self.log_table_schema)
                log_table_df.write.saveAsTable(self.full_log_table_name)

            processed_blobs_df = log_table_df.where(f"entity_name = '{entity_name}'").select(
                F.explode_outer("source_blobs").alias("source_blob")
            )

            print(f"{entity_name} - _get_entity_blobs - Removing already processed blobs...")

            # Subtract them from the list
            blobs_df = blobs_df.exceptAll(processed_blobs_df)

        blobs = blobs_df.collect()

        print(f"{entity_name} - _get_entity_blobs - Mode {mode} - Returning {len(blobs)} files")

        return [b.target_blob_url for b in blobs]

    def _enrich_with_option_map(self, entity_df: DataFrame, entity_name: str) -> DataFrame:
        """Enrich the raw dataset with string-mapped option sets.
        The string mapping are read by `OptionMapReaderJob` and then joined sequentally with the
        raw dataset.

        :param entity_df: :class:`DataFrame`
          The raw entity dataset
        :param entity_name: str
          The entity name
        :return: :class:`DataFrame`
          The enriched dataset.
        """
        # Read the option mapping dataset.
        sm_reader = OptionMapReaderJob(self.spark, self.environment)
        metadata_df = sm_reader.read_option_map(entity_name)
        if metadata_df.isEmpty():
            return entity_df

        # Get the list of impacted fields
        option_names = metadata_df.select("OptionSetName").distinct().collect()
        print(
            f"{entity_name} - enrich_with_option_map - "
            f"Options: [{','.join([o.OptionSetName for o in option_names])}]"
        )

        # Join sequentially on all the fields
        df = entity_df
        for option in option_names:
            option_df = metadata_df.where(metadata_df.OptionSetName == option.OptionSetName)
            df = (
                df.alias("df")
                .join(
                    option_df.alias(f"mdf_{option.OptionSetName}"),
                    df[option.OptionSetName] == option_df.Option,
                    "left",
                )
                .selectExpr(
                    "df.*",
                    f"mdf_{option.OptionSetName}.LocalizedLabel AS {option.OptionSetName}_descr",
                )
            )
        return df

    def _copy_to_destination(
        self, df: DataFrame, entity: CdmEntity, mode: str, processing_datetime: datetime
    ) -> str:
        """Ingest the entity dataset to the destination.

        This is an abstract method and not implemented in this class.

        :param df: :class:`DataFrame`
          The entity dataset
        :param entity: :class:`CdmEntity`
          The entity
        :param mode: str
          The ingestion mode
        :param processing_datetime: :class:`datetime`
          The ingestion processing datetime
        :return: str
          The destination where the dataset has been ingested
        """
        raise NotImplementedError

    def _build_log_entry(
        self,
        entity_name: str,
        destination: str,
        mode: str,
        process_dt: datetime,
        copy_start_dt: datetime,
        copy_end_dt: datetime,
        copy_duration_seconds: float,
        source_blobs: List[str],
    ) -> Row:
        """Build the log entry for the ingestion process.

        This is an abstract method and not implemented in this class.

        :param entity_name: str
          The entity name
        :param destination: str
          The ingestion destination
        :param mode: str
          The ingestion mode
        :param process_dt: :class:`datetime`
          The ingestion processing datetime
        :param copy_start_dt: :class:`datetime`
          The datatime when the ingestion started
        :param copy_end_dt: :class:`datetime`
          The datatime when the ingestion completed
        :param copy_duration_seconds: float
          The duration of the ingestion, in seconds
        :param source_blobs: List[str]
          The list of incremental file that have been ingested
        :return: :class:`Row`
          The log entry, as a :class:`Row`
        """
        raise NotImplementedError


class CdmToParquetIngestionJob(CdmBatchIngestionJob):
    """The implementation of `CdmBatchIngestionJob` for the parquet destination"""

    def __init__(self, spark: SparkSession, environment: Environment) -> None:
        """
        :param spark: :class:`SparkSession`
          Reference to the SparkSession
        :param environment: :class:`Environment`
          The class containing all the relevant environment-related variables
        """
        super().__init__(spark, environment)
        assert (
            self.environment.parquet_destination_root_path is not None
        ), "You must specify parquet_destination_root_path in your environment!"

    def _copy_to_destination(
        self, df: DataFrame, entity: CdmEntity, mode: str, processing_datetime: datetime
    ) -> str:
        """Ingest the entity dataset as a parquet folder.
        The folder path is build by combining:
        1. `parquet_destination_root_path` in `environment`
        2. The entity name
        3. The processing timestamp, e.g. 20240101_000000

        :param df: :class:`DataFrame`
          The entity dataset
        :param entity: :class:`CdmEntity`
          The entity
        :param mode: str
          The ingestion mode
        :param processing_datetime: :class:`datetime`
          The ingestion processing datetime
        :return: str
          The destination folder where the dataset has been saved
        """

        assert mode != self.MODE_UPSERT, "Mode upsert not supported by parquet destination."

        processing_datetime_str = processing_datetime.strftime("%Y%m%d_%H%M%S")
        dest_path = (
            f"{self.environment.parquet_destination_root_path}/{entity.name}/"
            f"{entity.name}_{processing_datetime_str}.parquet"
        )
        print(f"{entity.name} - copy_cdm_entity_to_destination - Writing to {dest_path}")
        df.write.parquet(dest_path)
        return dest_path

    def _build_log_entry(
        self,
        entity_name: str,
        destination: str,
        mode: str,
        process_dt: datetime,
        copy_start_dt: datetime,
        copy_end_dt: datetime,
        copy_duration_seconds: float,
        source_blobs: List[str],
    ) -> Row:
        """Build the log entry for the ingestion process.

        :param entity_name: str
          The entity name
        :param destination: str
          The ingestion destination
        :param mode: str
          The ingestion mode
        :param process_dt: :class:`datetime`
          The ingestion processing datetime
        :param copy_start_dt: :class:`datetime`
          The datatime when the ingestion started
        :param copy_end_dt: :class:`datetime`
          The datatime when the ingestion completed
        :param copy_duration_seconds: float
          The duration of the ingestion, in seconds
        :param source_blobs: List[str]
          The list of incremental file that have been ingested
        :return: :class:`Row`
          The log entry, as a :class:`Row`
        """
        return Row(
            entity_name=entity_name,
            destination=destination,
            destination_type="PATH",
            mode=mode,
            processing_dt=process_dt,
            copy_start_dt=copy_start_dt,
            copy_end_dt=copy_end_dt,
            copy_duration_seconds=copy_duration_seconds,
            source_blobs=source_blobs,
        )


class CdmToDeltaIngestionJob(CdmBatchIngestionJob):
    """The implementation of `CdmBatchIngestionJob` for the Delta table destination"""

    def __init__(self, spark: SparkSession, environment: Environment) -> None:
        """
        :param spark: :class:`SparkSession`
          Reference to the SparkSession
        :param environment: :class:`Environment`
          The class containing all the relevant environment-related variables
        """
        super().__init__(spark, environment)

        assert (
            self.environment.delta_destination_schema is not None
        ), "You must specify delta_destination_schema in your environment!"

        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.environment.delta_destination_schema}")

    def _copy_to_destination(
        self, df: DataFrame, entity: CdmEntity, mode: str, processing_datetime: datetime
    ) -> str:
        """Ingest the entity dataset to a Delta table.
        The table name is obtained by appending `_bronze` to the entity name, while the
        schema name is defined as `delta_destination_schema` in the `environment`.

        :param df: :class:`DataFrame`
          The entity dataset
        :param entity: :class:`CdmEntity`
          The entity
        :param mode: str
          The ingestion mode
        :param processing_datetime: :class:`datetime`
          The ingestion processing datetime (not used)
        :return: str
          The full destination table name
        """
        dest_table = f"{self.environment.delta_destination_schema}.{entity.name}_bronze"
        print(f"{entity.name} - copy_cdm_entity_to_destination - Writing to {dest_table}")

        if mode == self.MODE_FULL:
            df.write.saveAsTable(dest_table, mode="overwrite", overwriteSchema="true")

        try:
            self.spark.table(dest_table)
            upsert_is_append = False
        except:
            upsert_is_append = True

        if mode == self.MODE_APPEND or upsert_is_append:
            df.write.saveAsTable(dest_table, mode="append", mergeSchema="true")
        elif mode == self.MODE_UPSERT and not upsert_is_append:
            src_table = f"{entity.name}_src"
            df.createOrReplaceTempView(src_table)
            self.spark.sql(
                f"""MERGE INTO {dest_table} t
                    USING {src_table} s
                    ON t.Id = s.Id AND t.versionnumber = s.versionnumber
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *"""
            )
        else:
            pass

        return dest_table

    def _build_log_entry(
        self,
        entity_name: str,
        destination: str,
        mode: str,
        process_dt: datetime,
        copy_start_dt: datetime,
        copy_end_dt: datetime,
        copy_duration_seconds: float,
        source_blobs: List[str],
    ) -> Row:
        """Build the log entry for the ingestion process.

        :param entity_name: str
          The entity name
        :param destination: str
          The ingestion destination
        :param mode: str
          The ingestion mode
        :param process_dt: :class:`datetime`
          The ingestion processing datetime
        :param copy_start_dt: :class:`datetime`
          The datatime when the ingestion started
        :param copy_end_dt: :class:`datetime`
          The datatime when the ingestion completed
        :param copy_duration_seconds: float
          The duration of the ingestion, in seconds
        :param source_blobs: List[str]
          The list of incremental file that have been ingested
        :return: :class:`Row`
          The log entry, as a :class:`Row`
        """
        return Row(
            entity_name=entity_name,
            destination=destination,
            destination_type="TABLE",
            mode=mode,
            processing_dt=process_dt,
            copy_start_dt=copy_start_dt,
            copy_end_dt=copy_end_dt,
            copy_duration_seconds=copy_duration_seconds,
            source_blobs=source_blobs,
        )


class OptionMapReaderJob(object):
    """Job responsible for reading and parsing the CDM string-mapping for the entity options."""

    df_schema = T.StructType(
        [
            T.StructField("EntityName", T.StringType()),
            T.StructField("IsUserLocalizedLabel", T.BooleanType()),
            T.StructField("LocalizedLabel", T.StringType()),
            T.StructField("LocalizedLabelLanguageCode", T.StringType()),
            T.StructField("Option", T.LongType()),
            T.StructField("OptionSetName", T.StringType()),
        ]
    )
    """The JSON file schema"""

    def __init__(self, spark: SparkSession, environment: Environment) -> None:
        """
        :param spark: :class:`SparkSession`
          Reference to the SparkSession
        :param environment: :class:`Environment`
          The class containing all the relevant environment-related variables
        """
        self.spark = spark
        self.environment = environment

    def read_option_map(self, entity_name: str, label_language_code: int = 1040) -> DataFrame:
        """Read the options string-mapping file, combining in the same dataset both the entity options
        and the global options.
        The dataset can be optionally patched, for example to rename some options.

        :param entity_name: str
          The entity name
        :param label_language_code: int (optional)
          The option string language code
        :return: :class:`DataFrame`
          The persisted spark :class:`DataFrame` with the string-mappings.
        """
        try:
            metadata_path = (
                f"{self.environment.cdm_root_path}/Microsoft.Athena.TrickleFeedService/"
                f"{entity_name}-EntityMetadata.json"
            )
            source_df = self.spark.read.format("json").load(metadata_path)
        except:
            print(f"{entity_name} - read_option_map - Metadata not found!")
            return self.spark.createDataFrame([], self.df_schema)

        options_df = source_df.select(F.explode("OptionSetMetadata").alias("OptionSetMetadata"))
        options_cnt = options_df.count()

        global_options_df = source_df.select(
            F.explode("GlobalOptionSetMetadata").alias("GlobalOptionSetMetadata")
        )
        global_options_cnt = global_options_df.count()

        print(f"{entity_name} - read_option_map - Global Options count = {global_options_cnt}")
        print(f"{entity_name} - read_option_map - Options count = {options_cnt}")

        metadata_df = self.spark.createDataFrame([], schema=self.df_schema)

        if options_cnt > 0:
            metadata_df = metadata_df.union(
                options_df.select(
                    F.col("OptionSetMetadata.EntityName").alias("EntityName"),
                    F.col("OptionSetMetadata.IsUserLocalizedLabel").alias("IsUserLocalizedLabel"),
                    F.col("OptionSetMetadata.LocalizedLabel").alias("LocalizedLabel"),
                    F.col("OptionSetMetadata.LocalizedLabelLanguageCode").alias("LocalizedLabelLanguageCode"),
                    F.col("OptionSetMetadata.Option").alias("Option"),
                    F.col("OptionSetMetadata.OptionSetName").alias("OptionSetName"),
                ).where(F.col("LocalizedLabelLanguageCode") == label_language_code)
            )
        if global_options_cnt > 0:
            metadata_df = metadata_df.union(
                global_options_df.select(
                    F.lit(entity_name).alias("EntityName"),
                    F.col("GlobalOptionSetMetadata.IsUserLocalizedLabel").alias("IsUserLocalizedLabel"),
                    F.col("GlobalOptionSetMetadata.LocalizedLabel").alias("LocalizedLabel"),
                    F.col("GlobalOptionSetMetadata.LocalizedLabelLanguageCode").alias(
                        "LocalizedLabelLanguageCode"
                    ),
                    F.col("GlobalOptionSetMetadata.Option").alias("Option"),
                    F.col("GlobalOptionSetMetadata.OptionSetName").alias("OptionSetName"),
                ).where(F.col("LocalizedLabelLanguageCode") == label_language_code)
            )

        metadata_df = self.patch_dataframe(metadata_df, entity_name)

        metadata_df = metadata_df.distinct().persist()
        print(f"{entity_name} - read_option_map - Combined Options count = {metadata_df.count()}")

        return metadata_df

    def patch_dataframe(self, df: DataFrame, entity_name: str) -> DataFrame:
        """Patch the option dataset, based on the entity.

        :param df: :class:`DataFrame`
          The option dataset
        :param entity_name: str
          The entity name
        :return: :class:`DataFrame`
          The patched dataframe
        """
        patched_df = df

        # Implement here a custom logic to patch the option map based on the entity

        return patched_df
