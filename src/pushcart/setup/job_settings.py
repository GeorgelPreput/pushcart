import json
import logging
import operator
from functools import cache, lru_cache
from pathlib import Path
from typing import Any, Union

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.sdk.api_client import ApiClient
from pydantic import Json, constr, dataclasses, validator

from pushcart.validation.common import (
    PydanticArbitraryTypesConfig,
    validate_databricks_api_client,
)


@cache
def _get_smallest_cluster_node_type(client: ApiClient = None) -> str:
    cluster_api = ClusterApi(client)

    node_types = [
        t
        for t in cluster_api.list_node_types()["node_types"]
        if t["is_deprecated"] is False
        and t["is_hidden"] is False
        and t["photon_driver_capable"] is True
        and t["photon_worker_capable"] is True
    ]
    return sorted(
        node_types, key=operator.itemgetter("num_cores", "memory_mb", "num_gpus")
    )[0]["node_type_id"]


@cache
def _get_newest_spark_version(client: ApiClient = None) -> str:
    cluster_api = ClusterApi(client)

    spark_versions = [
        v
        for v in cluster_api.spark_versions()["versions"]
        if "ML" not in v["name"] and "LTS" in v["name"]
    ]
    return sorted(
        spark_versions,
        key=lambda x: float(x["name"].split(" LTS ")[0]),
        reverse=True,
    )[0]["key"]


@lru_cache(maxsize=50)
def _get_existing_cluster_id(
    client: ApiClient = None, cluster_name: constr(min_length=1, strict=True) = None
) -> str:
    cluster_api = ClusterApi(client)

    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    clusters = cluster_api.list_clusters().get("clusters", [])
    clusters_filtered = [c for c in clusters if c["cluster_name"] == cluster_name]

    if not clusters_filtered:
        log.warning(f"Cluster not found: {cluster_name}")
        return None

    cluster_id = clusters_filtered[0]["cluster_id"]
    log.info(f"Cluster ID: {cluster_id}")

    return cluster_id


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class JobSettings:
    client: ApiClient

    @validator("client")
    @classmethod
    def check_api_client(cls, value):
        return validate_databricks_api_client(value)

    def __post_init_post_parse__(self):
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.INFO)

    def load_job_settings(
        self,
        settings_json: Union[Json[Any], Path] = None,
        default_settings: constr(
            min_length=1, strict=True, regex=r"^(checkpoint|pipeline|release)$"
        ) = None,
    ) -> dict:
        job_settings = None

        if settings_json:
            job_settings = self._get_json_from_string() or self._get_json_from_file()

        if not job_settings:
            self.log.info("Creating release job using default settings")
            job_settings = self._get_default_job_settings(default_settings)

        return job_settings

    def _get_json_from_string(self) -> dict:
        try:
            return json.loads(self.settings_json)
        except (json.JSONDecodeError, TypeError):
            return None

    def _get_json_from_file(self) -> dict:
        try:
            if json_path := self.settings_json.resolve().as_posix():
                with open(json_path, "r") as json_file:
                    return json.load(json_file)
        except (
            FileNotFoundError,
            json.JSONDecodeError,
            OSError,
            RuntimeError,
            TypeError,
        ):
            return None

    def _get_default_job_settings(
        self,
        settings_name: constr(
            min_length=1, strict=True, regex=r"^(checkpoint|pipeline|release)$"
        ),
    ) -> dict:
        settings_map = {
            "checkpoint": _get_default_checkpoint_job_settings,
            "pipeline": _get_default_pipeline_job_settings,
            "release": _get_default_release_job_settings,
        }

        settings_getter = settings_map.get(settings_name)

        if not settings_getter:
            raise ValueError("Could not find default settings for {settings_name}")

        return settings_getter(self.client)


def _get_default_checkpoint_job_settings(client: ApiClient = None) -> dict:
    return {}


def _get_default_pipeline_job_settings(client: ApiClient = None) -> dict:
    return {}


def _get_default_release_job_settings(client: ApiClient = None) -> dict:
    return {
        "name": "release",
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "release",
                "python_wheel_task": {
                    "package_name": "pushcart",
                    "entry_point": "pushcart-release",
                    "named_parameters": {
                        "--workspace-url": "https://dbc-24f62d52-15b4.cloud.databricks.com/"
                    },
                },
                "job_cluster_key": "release",
                "libraries": [{"pypi": {"package": "pushcart"}}],
                "max_retries": 1,
                "min_retry_interval_millis": 15000,
                "retry_on_timeout": False,
                "timeout_seconds": 0,
                "email_notifications": {},
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "release",
                "new_cluster": {
                    "spark_version": _get_newest_spark_version(client),
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                    },
                    "node_type_id": _get_smallest_cluster_node_type(client),
                    "driver_node_type_id": _get_smallest_cluster_node_type(client),
                    "custom_tags": {"ResourceClass": "SingleNode"},
                    "enable_elastic_disk": True,
                    "runtime_engine": "PHOTON",
                    "num_workers": 0,
                },
            }
        ],
        "format": "MULTI_TASK",
    }
