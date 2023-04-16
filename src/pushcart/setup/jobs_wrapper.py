import logging
from pathlib import Path
from time import sleep
from typing import Optional, Tuple

from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient
from pydantic import dataclasses, validate_arguments, validator

from pushcart.setup.job_settings import JobSettings
from pushcart.validation.common import (
    PydanticArbitraryTypesConfig,
    validate_databricks_api_client,
)


@dataclasses.dataclass(config=PydanticArbitraryTypesConfig)
class JobsWrapper:
    """
    Manages Databricks jobs. It provides methods for creating, retrieving, and deleting
    jobs, as well as for running jobs and retrieving their status. It also uses the
    JobSettings class to load job settings from a JSON file or string, or to retrieve
    default job settings for checkpoint, pipeline, and release jobs.

    Fields:
    - client: instance of ApiClient used for interacting with the Databricks API
    - log: logger instance used for logging messages during job management
    - jobs_api: instance of JobsApi used for interacting with the Databricks Jobs API
    - runs_api: instance of RunsApi used for interacting with the Databricks Runs API
    """

    client: ApiClient

    @validator("client")
    @classmethod
    def check_api_client(cls, value):
        """
        Validator method for the provided ApiClient
        """
        return validate_databricks_api_client(value)

    def __post_init_post_parse__(self):
        """
        Initializes the logger instance and creates instances of JobsApi and RunsApi
        """
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.INFO)

        self.jobs_api = JobsApi(self.client)
        self.runs_api = RunsApi(self.client)

    @validate_arguments
    def get_or_create_release_job(self, settings_json: Optional[Path] = None) -> str:
        """
        Retrieves or creates a release job using the provided job settings
        """
        job_settings = JobSettings(self.client).load_job_settings(
            settings_json, "release"
        )

        return self.get_or_create_job(job_settings)

    def _get_job(self, job_name: str) -> list:
        """
        Retrieves a job by name
        """
        jobs = self.jobs_api.list_jobs().get("jobs", [])
        jobs_filtered = [j for j in jobs if j["settings"]["name"] == job_name]

        return jobs_filtered[0]["job_id"] if jobs_filtered else None

    def _create_job(self, job_settings: dict) -> str:
        """
        Creates a new job using the provided job settings
        """
        job = self.jobs_api.create_job(job_settings)
        self.log.info(f"Created job {job_settings['name']} with ID: {job['job_id']}")

        return job["job_id"]

    @validate_arguments
    def get_or_create_job(self, job_settings: dict) -> str:
        """
        Retrieves or creates a job using the provided job settings
        """
        job_name = job_settings.get("name")

        if not job_name:
            raise ValueError("Please provide a job name in the job settings")

        job_id = self._get_job(job_name)

        if not job_id:
            self.log.warning(f"Job not found: {job_name}. Creating a new one.")
            return self._create_job(job_settings)

        self.jobs_api.reset_job({"job_id": job_id, "new_settings": job_settings})

        self.log.info(f"Job ID: {job_id}")

        return job_id

    @validate_arguments
    def run_job(self, job_id: str) -> Tuple[str, str]:
        """
        Runs a job and retrieves its status
        """
        run_id = self.jobs_api.run_now(
            job_id=job_id,
            jar_params=None,
            notebook_params=None,
            python_params=None,
            spark_submit_params=None,
        )["run_id"]

        SLEEP_TIME = 2
        job_status = "PENDING"
        job = {}

        while job_status in ["PENDING", "RUNNING"]:
            sleep(SLEEP_TIME)
            job = self.runs_api.get_run(run_id)
            job_status = job["state"]["life_cycle_state"]

            logging.info(f"Job is {job_status}: {job['run_page_url']}")

        return (
            job["state"].get("result_state", job["state"]["life_cycle_state"]),
            job["run_page_url"],
        )

    @validate_arguments
    def delete_job(self, job_id: str) -> None:
        """
        Deletes a job by ID
        """
        job_name = self.jobs_api.get_job(job_id=job_id)["settings"]["name"]
        self.jobs_api.delete_job(job_id=job_id)

        logging.info(f"Deleted job {job_name} ({job_id})")
