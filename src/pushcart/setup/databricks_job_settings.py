import json
import logging
from importlib.resources import files
from pathlib import Path
from typing import Any, Union

from pydantic import Json, constr, dataclasses


@dataclasses.dataclass
class DatabricksJobSettings:
    settings_json: Union[Json[Any], Path] = None
    default_settings_package: constr(
        min_length=1, strict=True, regex=r"^[a-z*\.a-z*]+$"
    )
    default_settings_resource: constr(
        min_length=1, strict=True, regex=r"^[a-zA-Z0-9_]*\.json$"
    )

    def __post_init_post_parse__(self):
        self.log = logging.getLogger(__name__)

    def load_job_settings(self) -> dict:
        job_settings = None

        if self.settings_json:
            job_settings = self._get_json_from_string() or self._get_json_from_file()

        if not job_settings:
            self.log.info("Creating release job using default settings")
            self._get_json_from_package()

        return job_settings

    def _get_json_from_string(self) -> dict:
        job_settings = None

        try:
            job_settings = json.loads(self.settings_json)
        except (json.JSONDecodeError, TypeError):
            pass

        return job_settings

    def _get_json_from_file(self) -> dict:
        job_settings = None

        try:
            if json_path := self.settings_json.resolve().as_posix():
                with open(json_path, "r") as json_file:
                    job_settings = json.load(json_file)
        except (
            FileNotFoundError,
            json.JSONDecodeError,
            OSError,
            RuntimeError,
            TypeError,
        ):
            pass

        return job_settings

    def _get_json_from_package(self) -> dict:
        job_settings = {}
        with files(self.default_settings_package).joinpath(
            self.default_settings_resource
        ).open("r", encoding="utf-8") as settings_json:
            job_settings = json.load(settings_json)

        return job_settings
