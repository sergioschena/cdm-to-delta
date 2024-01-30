from azure.identity import ClientSecretCredential
from azure.storage.blob import ContainerClient

from pyspark.sql import types as T

from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Union, Optional, Dict
from typing_extensions import Self
import json


CdmAttributeType = Union[
    T.StringType,
    T.BooleanType,
    T.DecimalType,
    T.DoubleType,
    T.TimestampType,
    T.LongType,
]


def _build_dict_from_list(l) -> dict:
    return {i["name"]: i["value"] for i in l}


class Environment(object):
    target_path_suffix = "_incremental_csv_target"

    def __init__(
        self,
        *,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        source_account_name: str,
        source_container_name: str,
        target_account_name: str,
        target_container_name: str,
        cdm_root_path: str,
        log_schema_name: str,
        incremental_csv_container_path: str,
        parquet_destination_root_path: Optional[str] = None,
        delta_destination_schema: Optional[str] = None,
    ) -> None:
        self.credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        self.source_container_client = ContainerClient(
            Environment.account_url(source_account_name),
            source_container_name,
            self.credential,
        )
        self.target_container_client = ContainerClient(
            Environment.account_url(target_account_name),
            target_container_name,
            self.credential,
        )
        self.cdm_root_path = cdm_root_path.removesuffix("/")
        self.target_path_url = (
            f"{self.target_container_client.url}/{self.target_path_suffix}"
        )
        self.log_schema_name = log_schema_name
        self.incremental_csv_root_path = f"{incremental_csv_container_path.removesuffix('/')}/{self.target_path_suffix}"
        self.parquet_destination_root_path = parquet_destination_root_path
        self.delta_destination_schema = delta_destination_schema

    def get_storage_token(self):
        return self.credential.get_token("https://storage.azure.com/.default")

    @staticmethod
    def account_url(account_name: str) -> str:
        return f"https://{account_name}.blob.core.windows.net"


@dataclass
class SourcePartitionBlob:
    entity: str
    name: str
    blob_url: str
    offset: int


@dataclass
class ManagedBlobLogEntry:
    entity_name: str
    partition_name: str
    source_blob_url: str
    target_blob_url: str
    offset_start: int
    offset_end: int
    blob_length: int
    process_ts: datetime
    error: Optional[str]

    @classmethod
    def from_source_blob(
        cls,
        source_blob: SourcePartitionBlob,
        target_blob_url: str,
        blob_length: int,
        process_ts: datetime,
        error: Optional[str],
    ) -> Self:
        return cls(
            source_blob.entity,
            source_blob.name,
            source_blob.blob_url,
            target_blob_url,
            source_blob.offset,
            source_blob.offset + blob_length,
            blob_length,
            process_ts,
            error,
        )


class CdmAttribute(object):
    def __init__(
        self, name: str, data_type: str, schema_type: CdmAttributeType
    ) -> None:
        self.name = name
        self.data_type = data_type
        self.schema_type = schema_type

    @classmethod
    def from_dict(cls, d: dict) -> Self:
        name = d["name"]
        data_type = d["dataType"]
        traits = d.get("cdm:traits", [])
        scale = None
        precision = None

        if data_type == "decimal":
            args_dict = _build_dict_from_list(
                next(
                    (
                        t["arguments"]
                        for t in traits
                        if t["traitReference"] == "is.dataFormat.numeric.shaped"
                    ),
                    [],
                )
            )
            if "precision" in args_dict:
                precision = args_dict["precision"]
            if "scale" in args_dict:
                scale = args_dict["scale"]

        schema_type = CdmAttribute.get_schema_type(data_type, scale, precision)

        return cls(name, data_type, schema_type)

    def __repr__(self):
        t = (
            f"decimal({self.schema_type.precision},{self.schema_type.scale})"
            if self.data_type == "decimal"
            else self.data_type
        )
        return f"CdmAttribute({self.name}, {self.data_type}, {self.schema_type})"

    @staticmethod
    def get_schema_type(data_type, scale, precision) -> CdmAttributeType:
        if "guid" == data_type:
            return T.StringType()
        elif "boolean" == data_type:
            return T.BooleanType()
        elif "decimal" == data_type:
            if scale and precision:
                return T.DecimalType(precision=precision, scale=scale)
            else:
                return T.DecimalType()
        elif "double" == data_type:
            return T.DoubleType()
        elif "dateTimeOffset" == data_type:
            # TODO: set the right type here
            return T.TimestampType()
        elif "dateTime" == data_type:
            return T.TimestampType()
        elif "int64" == data_type:
            return T.LongType()
        elif "string" == data_type:
            return T.StringType()
        else:
            print(f"Data type mapping undefined for {data_type}. Defaulting to string")
            return T.StringType()


@dataclass
class CdmPartition:
    name: str
    entity: "CdmEntity"
    url: str = field(init=False)
    relative_path: str = field(init=False)
    full_path: str = field(init=False)

    def __post_init__(self):
        self._init_partition_info()


class CdmEntity(object):
    def __init__(
        self,
        name: str,
        annotations: dict,
        attributes: List[CdmAttribute],
        initial_sync_state: str,
        schema: T.StructType,
        manifest: "CdmManifest",
    ):
        self.name = name
        self.annotations = annotations
        self.attributes = attributes
        self.initial_sync_state = initial_sync_state
        self.schema = schema
        self.manifest = manifest

    @classmethod
    def from_dict(cls, d: dict, manifest: "CdmManifest") -> Self:
        name = d["name"]
        annotations = _build_dict_from_list(d.get("annotations", []))
        attributes = [CdmAttribute.from_dict(a) for a in d.get("attributes", [])]
        initial_sync_state = annotations.get("Athena:InitialSyncState", "Unknown")
        schema = T.StructType()
        for a in attributes:
            schema.add(a.name, a.schema_type)

        return cls(name, annotations, attributes, initial_sync_state, schema, manifest)

    def __repr__(self):
        return f"CdmEntity({self.name})"


class CdmManifest(object):
    def __init__(
        self,
        environment: "Environment",
        include_entities: List[str] = [],
    ):
        self.manifest_path = f"{environment.cdm_root_path}/model.json"
        self.entities = dict()
        self.include_entities = include_entities
        self._is_initialized = False

    def load_entities(self):
        print(f"Loading manifest from {self.manifest_path}")
        self._parse_manifest_json()
        self._is_initialized = True

    def get_entities(self) -> Dict[str, CdmEntity]:
        if not self._is_initialized:
            self.load_entities()
        return self.entities

    def get_entity(self, entity_name) -> CdmEntity:
        if not self._is_initialized:
            self.load_entities()
        return self.entities[entity_name]

    def _parse_manifest_json(self):
        with open(self.manifest_path, "r") as f:
            parsed = json.load(f)
            # print(parsed)
            for e in parsed["entities"]:
                if not self.include_entities or e["name"] in self.include_entities:
                    self._parse_entity(e)

    def _parse_entity(self, entity_dict: dict):
        entity = CdmEntity.from_dict(entity_dict, self)
        if entity.initial_sync_state == "Completed":
            print(f"Entity [{entity.name}] added !")
            self.entities[entity.name] = entity
