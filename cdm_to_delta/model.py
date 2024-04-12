from azure.identity import ClientSecretCredential
from azure.core.credentials import AccessToken
from azure.storage.blob import ContainerClient

from pyspark.sql import types as T

from datetime import datetime
from dataclasses import dataclass
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
"""Python type annotation for the possible types of a CDM field"""


def _build_dict_from_list(l) -> dict:  # noqa: E741
    """Aggregate in a single dictionary a list of key-value pairs."""
    return {i["name"]: i["value"] for i in l}


class Environment(object):
    """Container for storing all environment-level configurations, such as Azure Blob Storage credentials
    and container names, CDM root path.
    It also include some utility methods to build token for storage authentication and path mapping.
    """

    target_path_suffix = "_incremental_csv_target"
    """Root path for the incremental file in the destination container"""

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
        """
        :param tenant_id: str
          The service principal tenant ID
        :param client_id: str
          The service principal client ID
        :param client_secret: str
          The service principal secret
        :param source_account_name: str
          The name of the Dataverse Link storage account
        :param source_container_name: str
          The name of the Dataverse Link container
        :param target_account_name: str
          The name of the incremental files storage account
        :param target_container_name: str
          The name of the incremental files container
        :param cdm_root_path: str
          The root path to the CDM container in Databricks
        :param log_schema_name: str
          The name of the schema for the checkpoints log tables
        :param incremental_csv_container_path: str
          The root path to the the incremental files container in Databricks
        :param parquet_destination_root_path: str (optional)
          The root destination path for the parquet ingestion job
        :param delta_destination_schema: str (optional)
          The destination schema for the Delta table ingestion job
        """
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
        self.target_path_url = f"{self.target_container_client.url}/{self.target_path_suffix}"
        self.log_schema_name = log_schema_name
        self.incremental_csv_root_path = (
            f"{incremental_csv_container_path.removesuffix('/')}/{self.target_path_suffix}"
        )
        self.parquet_destination_root_path = parquet_destination_root_path
        self.delta_destination_schema = delta_destination_schema

    def get_storage_token(self) -> AccessToken:
        """Obtain an OAuth access token for the storage account.

        :return: :class:`AccessToken`
          The OAuth access token
        """
        return self.credential.get_token("https://storage.azure.com/.default")

    def blob_url_to_path(self, blob_url: str) -> str:
        """Transform the incremental file blob from the HTTPS storage account path
        to a Databricks path inside `incremental_csv_root_path`.

        :param blob_url: str
          The file URL
        :return: str
          The file path in Databricks
        """
        return (
            f"{self.incremental_csv_root_path}/"
            f"{blob_url.removeprefix(self.target_path_url).removeprefix('/')}"
        )

    @staticmethod
    def account_url(account_name: str) -> str:
        """Build the storage account URL from the storage account name

        :param account_name: str
          The storage account name
        :return: str
          The storage account URL
        """
        return f"https://{account_name}.blob.core.windows.net"


@dataclass
class SourcePartitionBlob:
    entity: str
    """The entity name"""

    name: str
    """The partition name"""

    blob_url: str
    """The partition file's URL"""

    offset: int
    """The latest offset in the file"""


@dataclass
class ManagedBlobLogEntry:
    entity_name: str
    """The entity name"""

    partition_name: str
    """The partition name"""

    source_blob_url: str
    """The partition file's URL"""

    target_blob_url: str
    """The incremental file's URL"""

    offset_start: int
    """The start offset in the source file"""

    offset_end: int
    """The end offset in the source file"""

    blob_length: int
    """The target file size"""

    process_ts: datetime
    """The process timestamp"""

    error: Optional[str]
    """The copy process error (optional)"""

    @classmethod
    def from_source_blob(
        cls,
        source_blob: SourcePartitionBlob,
        target_blob_url: str,
        blob_length: int,
        process_ts: datetime,
        error: Optional[str],
    ) -> Self:
        """Build a log entry from the source blob metadata and the copy process metadata.

        :param source_blob: :class:`SourcePartitionBlob`
          The source blob metadata
        :param target_blob_url: str
          The incremental file URL
        :param blob_length: int
          The incremental file length
        :param process_ts: :class:`datetime`
          The process timestamp
        :param error: Optional[str]
          The copy process error
        :return: :class:`ManagedBlobLogEntry`
          The log entry
        """
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
    def __init__(self, name: str, data_type: str, schema_type: CdmAttributeType) -> None:
        """
        :param name: str
          The attribute name
        :param data_type: str
          The attribute data type
        :param schema_type: `CdmAttributeType`
          The attribute spark data type
        """
        self.name = name
        self.data_type = data_type
        self.schema_type = schema_type

    @classmethod
    def from_dict(cls, d: dict) -> Self:
        """Build a :class:`CdmAttribute` parsing the JSON object from the `model.json` file.

        :param d: dict
          The JSON object of the attribute
        :return: :class:`CdmAttribute`
          The attribute object
        """
        name = d["name"]
        data_type = d["dataType"]
        traits = d.get("cdm:traits", [])
        scale = None
        precision = None

        if data_type == "decimal":
            args_dict = _build_dict_from_list(
                next(
                    (t["arguments"] for t in traits if t["traitReference"] == "is.dataFormat.numeric.shaped"),
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
        return f"CdmAttribute({self.name}, {self.data_type}, {self.schema_type})"

    @staticmethod
    def get_schema_type(data_type: str, scale: Optional[str], precision: Optional[str]) -> CdmAttributeType:
        """Map the CDM attribute type to the corresponding spark data type

        :param data_type: str
          The attribute data type
        :param scale: int (optional)
          The decimal scale
        :param precision: int (optional)
          The decimal precision
        :return: `CdmAttributeType`
          The spark data type object
        """
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


class CdmEntity(object):
    def __init__(
        self,
        name: str,
        annotations: dict,
        attributes: List[CdmAttribute],
        initial_sync_state: str,
        schema: T.StructType,
        manifest: "CdmManifest",
    ) -> None:
        """
        :param name: str
          The entity name
        :param annotations: dict
          The entity annotations dictionary
        :param attributes: List[:class:`CdmAttribute`]
          The list of entity attributes
        :param initial_sync_state: str
          The entity synchronization status
        :param schema: :class:`T.StructType`
          The entity spark schema
        :param manifest: :class:`CdmManifest`
          The reference to the manifest file object
        """
        self.name = name
        self.annotations = annotations
        self.attributes = attributes
        self.initial_sync_state = initial_sync_state
        self.schema = schema
        self.manifest = manifest

    @classmethod
    def from_dict(cls, d: dict, manifest: "CdmManifest") -> Self:
        """Build a :class:`CdmEntity` parsing the JSON object from the `model.json` file.

        :param d: dict
          The entity JSON object
        :param manifest: :class:`CdmManifest`
          The reference to the manifest object
        :return: :class:`CdmEntity`
          The entity object
        """
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
    """Object representation of the `model.json` file.
    It is a container for all the :class:`CdmEntity` objects parsed from the file.
    By default, all the entities from the manifest file are parsed, but you can decide
    to parse only a subset of them.

    Note that the file is lazily parsed and the entities with a sync status other than
    `Completed` are ignored.
    """

    def __init__(
        self,
        environment: "Environment",
        include_entities: List[str] = [],
    ):
        """
        :param environment: :class:`Environment`
          The class containing all the relevant environment-related variables
        :param include_entities: List[str] (optional)
          The list of entities to parse. It defaults to an empty list (all the entities are parsed).
        """
        self.manifest_path = f"{environment.cdm_root_path}/model.json"
        self.entities = dict()
        self.include_entities = include_entities
        self._is_initialized = False

    def load_entities(self):
        """Load the entities, parsing the manifest file"""
        print(f"Loading manifest from {self.manifest_path}")
        self._parse_manifest_json()
        self._is_initialized = True

    def get_entities(self) -> Dict[str, CdmEntity]:
        """Getter of the entire dictionary of entities.
        The key is the entity name, while the value is an instance of :class:`CdmEntity`.

        :return: Dict[str, :class:`CdmEntity`]
          The entities dictionary.
        """
        if not self._is_initialized:
            self.load_entities()
        return self.entities

    def get_entity(self, entity_name: str) -> CdmEntity:
        """Get the instance of :class:`CdmEntity` for a given entity (by name).

        :param entity_name: str
          The entity name
        :return: Dict[str, :class:`CdmEntity`]
          The entity object
        """
        if not self._is_initialized:
            self.load_entities()
        return self.entities[entity_name]

    def _parse_manifest_json(self):
        """Read and parse the manifest file, populating the entity instances dictionary."""
        with open(self.manifest_path, "r") as f:
            parsed = json.load(f)
            # print(parsed)
            for e in parsed["entities"]:
                if not self.include_entities or e["name"] in self.include_entities:
                    self._parse_entity(e)

    def _parse_entity(self, entity_dict: dict):
        """Parse the entity JSON object and add it to the dictionary.

        :param entity_dict: dict
          The entity JSON object
        """
        entity = CdmEntity.from_dict(entity_dict, self)
        if entity.initial_sync_state == "Completed":
            print(f"Entity [{entity.name}] added !")
            self.entities[entity.name] = entity
