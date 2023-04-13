import logging
from pathlib import Path
from time import sleep
from typing import Any, Tuple, Union

from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient
from pydantic import Json, dataclasses, validator

from pushcart.setup.job_settings import JobSettings
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
        self.runs_api = RunsApi(self.client)

    def get_or_create_release_job(
        self, settings_json: Union[Json[Any], Path] = None
    ) -> str:
        job_settings = JobSettings(self.client).load_job_settings(
            settings_json, "release"
        )

        return self.get_or_create_job(job_settings)

    def _get_job(self, job_name: str) -> list:
        jobs = self.jobs_api.list_jobs().get("jobs", [])
        jobs_filtered = [j for j in jobs if j["settings"]["name"] == job_name]

        return jobs_filtered[0]["job_id"] if jobs_filtered else None

    def _create_job(self, job_settings: dict) -> str:
        job = self.jobs_api.create_job(job_settings)
        self.log.info(f"Created job {job_settings['name']} with ID: {job['job_id']}")

        return job["job_id"]

    def get_or_create_job(self, job_settings: dict) -> str:
        job_name = job_settings["name"]

        job_id = self._get_job(job_name)

        if not job_id:
            self.log.warning(f"Job not found: {job_name}. Creating a new one.")
            return self._create_job(job_settings)

        self.jobs_api.reset_job({"job_id": job_id, "new_settings": job_settings})

        self.log.info(f"Job ID: {job_id}")

        return job_id

    def run_job(self, job_id: str) -> Tuple[str, str]:
        run_id = self.jobs_api.run_now(
            job_id=job_id,
            jar_params=None,
            notebook_params=None,
            python_params=None,
            spark_submit_params=None,
        )["run_id"]

        job_status = "PENDING"

        job = {}
        while job_status in ["PENDING", "RUNNING"]:
            sleep(2)
            job = self.runs_api.get_run(run_id)
            job_status = job["state"]["life_cycle_state"]

            logging.info(f"Job is {job_status}: {job['run_page_url']}")

        return (
            job["state"].get("result_state", job["state"]["life_cycle_state"]),
            job["run_page_url"],
        )

    def delete_job(self, job_id: str) -> None:
        job_name = self.jobs_api.get_job(job_id=job_id)["settings"]["name"]
        self.jobs_api.delete_job(job_id=job_id)

        logging.info(f"Deleted job {job_name} ({job_id})")
