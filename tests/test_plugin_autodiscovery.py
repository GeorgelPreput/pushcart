from datetime import datetime
from unittest.mock import MagicMock

import pytest

# from pushcart.stages.autodiscovery import _get_installed_packages, get_stage_object
from pushcart.plugins import _get_installed_packages, get_stage_object
from pushcart.stages.sources.autoloader import AutoloaderSource
from pushcart_rest_api.stages.sources import RestApiSource


class TestGetStageObject:
    def test_get_stage_object_returns_existing_object(self):
        source = get_stage_object(
            "sources", config={"type": "autoloader"}, run_ts=datetime.now()
        )

        assert isinstance(source, AutoloaderSource)

    def test_get_stage_object_fails_on_inexistent_stage(self):
        with pytest.raises(ValueError) as e:
            _ = get_stage_object(
                "no_such_stage", config={"type": "doesnt_matter"}, run_ts=datetime.now()
            )
        assert str(e.value) == "Unknown stage: no_such_stage"

    def test_get_stage_object_fails_on_inexistent_object_type(self):
        with pytest.raises(ValueError) as e:
            _ = get_stage_object(
                "sources", config={"type": "no_such_object_type"}, run_ts=datetime.now()
            )
        assert str(e.value) == "Unknown sources type: no_such_object_type"

    def test_get_stage_object_returns_extra_package_object(self):
        source = get_stage_object(
            "sources", config={"type": "rest_api"}, run_ts=datetime.now()
        )

        assert isinstance(source, RestApiSource)

    def test_get_stage_object_fails_on_invalid_stage_with_extra_package(self):
        with pytest.raises(ValueError) as e:
            _ = get_stage_object(
                "no_such_stage",
                config={"type": "rest_api"},
                run_ts=datetime.now(),
            )
        assert str(e.value) == "Unknown stage: no_such_stage"

    def test_get_stage_object_fails_on_invalid_extra_package(self):
        with pytest.raises(ValueError) as e:
            _ = get_stage_object(
                "sources",
                config={"type": "no_such_extra_package"},
                run_ts=datetime.now(),
            )
        assert str(e.value) == "Unknown sources type: no_such_extra_package"


class Test_GetInstalledPackages:
    def test_get_installed_packages_ignores_invalid_package_names(self, mocker):
        invalid_package_name = "invalid_package_name"
        valid_package_name = "pushcart_rest_api"

        mock_working_set = [
            MagicMock(key=invalid_package_name),
            MagicMock(key=valid_package_name),
        ]

        mocker.patch("pkg_resources.working_set", mock_working_set)

        installed_packages = _get_installed_packages("stages.sources")

        assert invalid_package_name not in str(installed_packages)
        assert valid_package_name in str(installed_packages)
