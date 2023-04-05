import logging
import operator
from functools import cache
from pathlib import Path
from typing import Any, Tuple, Union

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient
from pydantic import Json, dataclasses, validator

from pushcart.setup.databricks_job_settings import DatabricksJobSettings
from pushcart.validation.common import (
    PydanticArbitraryTypesConfig,
    validate_databricks_api_client,
)


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class JobsWrapper:
    client: ApiClient

    @validator("client")
    @classmethod
    def check_api_client(cls, value):
        return validate_databricks_api_client(value)

    def __post_init_post_parse__(self):
        self.log = logging.getLogger(__name__)

        self.jobs_api = JobsApi(self.client)
        self.cluster_api = ClusterApi(self.client)

    def get_or_create_release_job(self, settings_json: Union[Json[Any], Path] = None):
        djs = DatabricksJobSettings(settings_json, "pushcart.setup", "release_job.json")
        raw_job_settings = djs.load_job_settings()
        job_settings = self._add_spark_version_and_node_type(raw_job_settings)

        self.get_or_create_job(job_settings)

    @cache
    def _get_smallest_cluster_node_type(self) -> str:
        node_types = [
            t
            for t in self.cluster_api.list_node_types()["node_types"]
            if t["is_deprecated"] is False
            and t["is_hidden"] is False
            and t["photon_driver_capable"] is True
            and t["photon_worker_capable"] is True
        ]
        return sorted(
            node_types, key=operator.itemgetter("num_cores", "memory_mb", "num_gpus")
        )[0]["node_type_id"]

    @cache
    def _get_newest_spark_version(self) -> str:
        spark_versions = [
            v
            for v in self.cluster_api.spark_versions()["versions"]
            if "LTS Photon" in v["name"]
        ]
        return sorted(
            spark_versions,
            key=lambda x: float(x["name"].split(" LTS Photon ")[0]),
            reverse=True,
        )[0]["key"]

    def _add_spark_version_and_node_type(self, job_settings: dict) -> dict:
        if not (job_clusters := job_settings.get("job_clusters")):
            raise ValueError(
                "Job cluster specification must include at least one cluster"
            )

        for cluster in job_clusters:
            cluster["spark_version"] = (
                cluster["spark_version"] or self._get_newest_spark_version()
            )
            cluster["node_type_id"] = (
                cluster["node_type_id"] or self._get_smallest_cluster_node_type()
            )

        return job_settings

    def get_or_create_job(self, job_settings: dict) -> str:
        job_name = job_settings["name"]

        jobs = self.jobs_api.list_jobs().get("jobs", [])
        jobs_filtered = [j for j in jobs if j["settings"]["name"] == job_name]

        if not jobs_filtered:
            self.log.warning(f"Job not found: {job_name}. Creating a new one.")

            job = self.jobs_api.create_job(job_settings)
            self.log.info(f"Created job {job_name} with ID: {job['job_id']}")

            return job["job_id"]

        job_id = jobs_filtered[0]["job_id"]
        self.log(f"Job ID: {job_id}")

        return job_id

    def run_job(self, job_id: str) -> Tuple[str, str]:
        # TODO: Check how to obfuscate token in run params
        pass
