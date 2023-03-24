import importlib
import inspect
import pkgutil
import re
from datetime import datetime
from functools import lru_cache

import pkg_resources

from pushcart.stages.destinations.destination_base import DestinationBase
from pushcart.stages.sources.source_base import SourceBase
from pushcart.stages.transformations.transformation_base import TransformationBase


def _recursive_import(module, base_class, classes):
    for _, name, ispkg in pkgutil.iter_modules(module.__path__):
        submodule_name = f"{module.__name__}.{name}"
        try:
            submodule = importlib.import_module(submodule_name)
            if ispkg:
                _recursive_import(submodule, base_class, classes)
            else:
                for _, cls in inspect.getmembers(submodule, inspect.isclass):
                    if issubclass(cls, base_class) and cls != base_class:
                        classes[name] = cls
        except ModuleNotFoundError:
            pass


@lru_cache(maxsize=None)
def __discover_classes(package_name, base_class):
    classes = {}

    print(package_name)
    try:
        package = importlib.import_module(package_name)
        _recursive_import(package, base_class, classes)
    except ModuleNotFoundError:
        pass

    return classes


@lru_cache(maxsize=None)
def __get_installed_packages():
    packages = {}
    for stage in ["sources", "transformations", "destinations"]:
        packages[stage] = []

        # Regular expression pattern to match the library names
        pattern = re.compile(r"pushcart(?:_?-?[a-z]+)*")

        for pkg in pkg_resources.working_set:
            if pattern.match(pkg.key):
                package_name = pkg.key.replace("-", "_")
                if package_name == "pushcart":
                    packages[stage].append(f"{package_name}.stages.{stage}")
                else:
                    packages[stage].append(f"{package_name}.{stage}")

    return packages


def get_stage_object(stage: str, config: dict, run_ts: datetime):
    base_classes = {
        "sources": SourceBase,
        "transformations": TransformationBase,
        "destinations": DestinationBase,
    }

    if stage not in base_classes:
        raise ValueError(f"Unknown stage: {stage}")

    stage_packages = __get_installed_packages()

    stage_classes = {}

    for package_name in stage_packages[stage]:
        try:
            stage_classes.update(__discover_classes(package_name, base_classes[stage]))
        except ImportError:
            pass

    if config["type"] in stage_classes:
        return stage_classes[config["type"]](config, run_ts)
    else:
        raise ValueError(f"Unknown {stage} type: {config['type']}")
