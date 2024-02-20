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

from cdm_to_delta.model import Environment

from .adls import copy_range_from_url
from .model import Environment, CdmEntity, ManagedBlobLogEntry, SourcePartitionBlob


class CdmPartitionIncrementalCopyJob(object):
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
    log_table_name = "managed_partition_blob_log"

    def __init__(self, spark: SparkSession, environment: Environment) -> None:
        self.spark = spark
        self.environment = environment
        self.partition_blob_log_table_name = (
            f"{self.environment.log_schema_name}.{self.log_table_name}"
        )

    def fetch_blob_state_from_log(self, entities: List[CdmEntity]) -> List[Row]:
        entity_names = [e.name for e in entities]

        w = Window.partitionBy("source_blob_url").orderBy(
            F.col("process_ts").desc(),
        )
        try:
            partition_blob_log_df = self.spark.table(self.partition_blob_log_table_name)
        except:
            print(
                f"CdmPartitionIncrementalCopyJob - {self.partition_blob_log_table_name} does not exist. Creating it..."
            )
            partition_blob_log_df = self.spark.createDataFrame(
                [], self.log_table_schema
            )
            partition_blob_log_df.write.saveAsTable(self.partition_blob_log_table_name)

        print(
            f"CdmPartitionIncrementalCopyJob - Reading latest blob state from {self.partition_blob_log_table_name}..."
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
        self, entities: List[CdmEntity], partition_rows: List[Row]
    ) -> List[ManagedBlobLogEntry]:

        managed_blob_state: Dict[str, Dict[str, SourcePartitionBlob]] = dict()
        for r in partition_rows:
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
                        f"CdmPartitionIncrementalCopyJob - Entity: {entity.name} - Content unchanged for {source_url}. Skipping copy"
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
        def copy_partition_blob(
            entry: ManagedBlobLogEntry,
            environment: Environment,
            source_token: str,
            index: int,
            total: int,
        ):
            print(
                f"CdmPartitionIncrementalCopyJob - {index+1}/{total} Entity: {entry.entity_name} - Copying {entry.blob_length} bytes from {entry.source_blob_url} to {entry.target_blob_url} - Range: {entry.offset_start}-{entry.offset_end}"
            )

            target_blob_client = BlobClient.from_blob_url(
                entry.target_blob_url, environment.credential
            )
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

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_concurrent_copies
        ) as executor:
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
        if not log_entries:
            return

        log_df = self.spark.createDataFrame(
            (Row(**asdict(l)) for l in log_entries), self.log_table_schema
        )
        log_df.write.saveAsTable(self.partition_blob_log_table_name, mode="append")


class CdmBatchIngestionJob(object):
    cdm_timestamp_format = "MM/dd/yyyy hh:mm:ss aa"
    escape = '"'
    delimiter = ","

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
    log_table_name = "cdm_to_raw_ingestion_log"

    MODE_APPEND = "append"
    MODE_UPSERT = "upsert"
    MODE_FULL = "full"
    MODES = [MODE_APPEND, MODE_FULL, MODE_UPSERT]

    def __init__(self, spark: SparkSession, environment: Environment) -> None:
        self.spark = spark
        self.environment = environment
        self.full_log_table_name = (
            f"{self.environment.log_schema_name}.{self.log_table_name}"
        )
        self.log_entries: List[Row] = list()
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        self.spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {self.environment.log_schema_name}"
        )

    def copy_cdm_entities_to_destination(
        self,
        *,
        entities: List[CdmEntity],
        mode: str = MODE_APPEND,
        update_log: bool = True,
        enrich_with_option_map: bool = True,
    ):
        if update_log:
            self.log_entries = list()

        for entity in entities:
            self.copy_cdm_entity_to_destination(
                entity=entity,
                mode=mode,
                update_log=False,
                enrich_with_option_map=enrich_with_option_map,
            )

        if update_log:
            self.persist_log_entries()

    def copy_cdm_entity_to_destination(
        self,
        *,
        entity: CdmEntity,
        mode: str = MODE_APPEND,
        update_log: bool = False,
        enrich_with_option_map: bool = True,
    ) -> None:
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

        destination = self._copy_to_destination(
            entity_df, entity, mode, processing_datetime
        )

        copy_end_time = datetime.now()
        copy_duration = (copy_end_time - copy_start_time).total_seconds()
        print(
            f"{entity.name} - copy_cdm_entity_to_destination - Copy took {copy_duration:0.3f}s"
        )

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
        blobs = self._get_entity_blobs(entity.name, mode)
        paths = [self._blob_url_to_path(b) for b in blobs]
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
                    escape=self.escape,
                    header=False,
                    sep=self.delimiter,
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
        if not self.log_entries:
            return
        log_df = self.spark.createDataFrame(self.log_entries, self.log_table_schema)
        log_df.write.saveAsTable(self.full_log_table_name, mode="append")

    def _get_entity_blobs(self, entity_name: str, mode: str) -> List[str]:

        assert (
            mode in self.MODES
        ), f"Mode {mode} is not supported. Choose one of {','.join(self.MODES)}"

        copy_job = CdmPartitionIncrementalCopyJob(self.spark, self.environment)

        print(f"{entity_name} - _get_entity_blobs - Fetching available blobs...")

        # full
        blobs_df = (
            self.spark.table(copy_job.partition_blob_log_table_name)
            .where(f"entity_name = '{entity_name}' AND error IS NULL")
            .select("target_blob_url")
        )

        if self.MODE_APPEND == mode:
            # Read blobs already processed
            try:
                log_table_df = self.spark.table(self.full_log_table_name)
            except:
                print(
                    f"{self.__class__.__name__} - {self.full_log_table_name} does not exist. Creating it..."
                )
                log_table_df = self.spark.createDataFrame([], self.log_table_schema)
                log_table_df.write.saveAsTable(self.full_log_table_name)

            processed_blobs_df = log_table_df.where(
                f"entity_name = '{entity_name}'"
            ).select(F.explode_outer("source_blobs").alias("source_blob"))

            print(
                f"{entity_name} - _get_entity_blobs - Removing already processed blobs..."
            )

            # Subtract them from the list
            blobs_df = blobs_df.exceptAll(processed_blobs_df)

        blobs = blobs_df.collect()

        print(
            f"{entity_name} - _get_entity_blobs - Mode {mode} - Returning {len(blobs)} files"
        )

        return [b.target_blob_url for b in blobs]

    def _enrich_with_option_map(
        self, entity_df: DataFrame, entity_name: str
    ) -> DataFrame:
        sm_reader = OptionMapReaderJob(self.spark, self.environment)
        metadata_df = sm_reader.read_option_map(entity_name)
        if metadata_df.isEmpty():
            return entity_df

        option_names = metadata_df.select("OptionSetName").distinct().collect()
        print(
            f"{entity_name} - enrich_with_option_map - Options: [{','.join([o.OptionSetName for o in option_names])}]"
        )

        df = entity_df
        for option in option_names:
            option_df = metadata_df.where(
                metadata_df.OptionSetName == option.OptionSetName
            )
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

    def _blob_url_to_path(self, blob_url: str) -> str:
        return f"{self.environment.incremental_csv_root_path}/{blob_url.removeprefix(self.environment.target_path_url).removeprefix('/')}"

    def _copy_to_destination(
        self, df: DataFrame, entity: CdmEntity, mode: str, processing_datetime: datetime
    ) -> str:
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
        raise NotImplementedError


class CdmToParquetIngestionJob(CdmBatchIngestionJob):
    def __init__(self, spark: SparkSession, environment: Environment) -> None:
        super().__init__(spark, environment)
        assert (
            self.environment.parquet_destination_root_path is not None
        ), "You must specify parquet_destination_root_path in your environment!"

    def _copy_to_destination(
        self, df: DataFrame, entity: CdmEntity, mode: str, processing_datetime: datetime
    ) -> str:

        assert (
            mode != self.MODE_UPSERT
        ), "Mode upsert not supported by parquet destination."

        processing_datetime_str = processing_datetime.strftime("%Y%m%d_%H%M%S")
        dest_path = f"{self.environment.parquet_destination_root_path}/{entity.name}/{entity.name}_{processing_datetime_str}.parquet"
        print(
            f"{entity.name} - copy_cdm_entity_to_destination - Writing to {dest_path}"
        )
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
    def __init__(self, spark: SparkSession, environment: Environment) -> None:
        super().__init__(spark, environment)

        assert (
            self.environment.delta_destination_schema is not None
        ), "You must specify delta_destination_schema in your environment!"

        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        self.spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {self.environment.delta_destination_schema}"
        )

    def _copy_to_destination(
        self, df: DataFrame, entity: CdmEntity, mode: str, processing_datetime: datetime
    ) -> str:
        dest_table = f"{self.environment.delta_destination_schema}.{entity.name}_bronze"
        print(
            f"{entity.name} - copy_cdm_entity_to_destination - Writing to {dest_table}"
        )

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
    df_schema = T._parse_datatype_string(
        "EntityName STRING, IsUserLocalizedLabel BOOLEAN, LocalizedLabel STRING, LocalizedLabelLanguageCode LONG, Option LONG, OptionSetName STRING"
    )

    def __init__(self, spark: SparkSession, environment: Environment) -> None:
        self.spark = spark
        self.environment = environment

    def read_option_map(
        self, entity_name: str, label_language_code: int = 1040
    ) -> DataFrame:
        try:
            metadata_path = f"{self.environment.cdm_root_path}/Microsoft.Athena.TrickleFeedService/{entity_name}-EntityMetadata.json"
            source_df = self.spark.read.format("json").load(metadata_path)
        except:
            print(f"{entity_name} - read_option_map - Metadata not found!")
            return self.spark.createDataFrame([], self.df_schema)

        options_df = source_df.select(
            F.explode("OptionSetMetadata").alias("OptionSetMetadata")
        )
        options_cnt = options_df.count()

        global_options_df = source_df.select(
            F.explode("GlobalOptionSetMetadata").alias("GlobalOptionSetMetadata")
        )
        global_options_cnt = global_options_df.count()

        print(
            f"{entity_name} - read_option_map - Global Options count = {global_options_cnt}"
        )
        print(f"{entity_name} - read_option_map - Options count = {options_cnt}")

        metadata_df = self.spark.createDataFrame([], schema=self.df_schema)

        if options_cnt > 0:
            metadata_df = metadata_df.union(
                options_df.select(
                    F.col("OptionSetMetadata.EntityName").alias("EntityName"),
                    F.col("OptionSetMetadata.IsUserLocalizedLabel").alias(
                        "IsUserLocalizedLabel"
                    ),
                    F.col("OptionSetMetadata.LocalizedLabel").alias("LocalizedLabel"),
                    F.col("OptionSetMetadata.LocalizedLabelLanguageCode").alias(
                        "LocalizedLabelLanguageCode"
                    ),
                    F.col("OptionSetMetadata.Option").alias("Option"),
                    F.col("OptionSetMetadata.OptionSetName").alias("OptionSetName"),
                ).where(F.col("LocalizedLabelLanguageCode") == label_language_code)
            )
        if global_options_cnt > 0:
            metadata_df = metadata_df.union(
                global_options_df.select(
                    F.lit(entity_name).alias("EntityName"),
                    F.col("GlobalOptionSetMetadata.IsUserLocalizedLabel").alias(
                        "IsUserLocalizedLabel"
                    ),
                    F.col("GlobalOptionSetMetadata.LocalizedLabel").alias(
                        "LocalizedLabel"
                    ),
                    F.col("GlobalOptionSetMetadata.LocalizedLabelLanguageCode").alias(
                        "LocalizedLabelLanguageCode"
                    ),
                    F.col("GlobalOptionSetMetadata.Option").alias("Option"),
                    F.col("GlobalOptionSetMetadata.OptionSetName").alias(
                        "OptionSetName"
                    ),
                ).where(F.col("LocalizedLabelLanguageCode") == label_language_code)
            )

        metadata_df = self.patch_dataframe(metadata_df, entity_name)

        metadata_df = metadata_df.distinct().persist()
        print(
            f"{entity_name} - read_option_map - Combined Options count = {metadata_df.count()}"
        )

        return metadata_df

    def patch_dataframe(self, df: DataFrame, entity_name: str) -> DataFrame:
        patched_df = df
        
        ## Implement here a custom logic to patch the option map based on the entity

        return patched_df
