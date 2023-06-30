"""Plugin discovery and importing module.

It provides functions to discover and import plugins for different stages (sources,
transformations, destinations) and log handlers.

Functions
---------
get_stage_object(stage, config, run_ts)
    Get a plugin object for a specific stage.
get_handler_object(config)
    Get a plugin object for a log handler.

"""

import contextlib
import importlib
import inspect
import logging
import pkgutil
import re
from datetime import datetime
from functools import cache
from importlib import metadata as ilib_meta
from types import ModuleType

from pushcart.stages.stage_base import StageBase


def _recursive_import(module: ModuleType, base_class: any, classes: dict) -> None:
    """Import a module along with its submodules in a recursive manner.

    Then, identify and add any classes that are derived from a specified base class into a dictionary.

    Parameters
    ----------
    module : ModuleType
        The module to import.
    base_class : any
        The base class to check for subclasses.
    classes : dict
        The dictionary to add the classes to.
    """
    for _, name, ispkg in pkgutil.iter_modules(module.__path__):
        submodule_name = f"{module.__name__}.{name}"
        with contextlib.suppress(ModuleNotFoundError, ImportError):
            submodule = importlib.import_module(submodule_name)
            if ispkg:
                _recursive_import(submodule, base_class, classes)
            else:
                for _, cls in inspect.getmembers(submodule, inspect.isclass):
                    if issubclass(cls, base_class) and cls != base_class:
                        classes[name] = cls


@cache
def _discover_classes(package_name: str, base_class: any) -> dict:
    """Discover and import classes from a package that are subclasses of a base class.

    Parameters
    ----------
    package_name : str
        The name of the package to import.
    base_class : any
        The base class to check for subclasses.

    Returns
    -------
    dict
        A dictionary mapping class names to class objects.
    """
    classes = {}

    with contextlib.suppress(ModuleNotFoundError):
        package = importlib.import_module(package_name)
        _recursive_import(package, base_class, classes)

    return classes


@cache
def _get_installed_packages(subpackage: str) -> list[str]:
    """Get a list of installed packages that match a specific subpackage.

    Parameters
    ----------
    subpackage : str
        The subpackage to match.

    Returns
    -------
    list[str]
        A list of package names.
    """
    packages = []

    # Regular expression pattern to match the library names
    pattern = re.compile(r"pushcart(?:_?-?[a-z]+)*")

    for pkg in ilib_meta.distributions():
        if pattern.match(pkg.name):
            package_name = pkg.key.replace("-", "_")
            if package_name == "pushcart":
                packages.append(f"{package_name}.{subpackage}")
            else:
                packages.append(f"{package_name}.{subpackage}")

    return packages


def _get_plugin_object(subpackage: str, plugin_class: any, config: dict) -> any:
    """Get a plugin object for a specific subpackage and base class.

    Parameters
    ----------
    subpackage : str
        The subpackage to match.
    plugin_class : any
        The base class to check for subclasses.
    config : dict
        The configuration for the plugin.

    Returns
    -------
    any
        The plugin object.

    Raises
    ------
    TypeError
        If the plugin type specified in the config is not found.
    """
    plugin_packages = _get_installed_packages(subpackage)

    plugin_classes = {}

    for package_name in plugin_packages:
        with contextlib.suppress(ImportError):
            plugin_classes.update(_discover_classes(package_name, plugin_class))

    if config["type"] in plugin_classes:
        return plugin_classes[config["type"]](config)

    msg = f"Unknown plugin type: {config['type']}"
    raise TypeError(msg)


def get_stage_object(stage: str, config: dict, run_ts: datetime) -> StageBase:
    """Get a plugin object for a specific stage.

    Parameters
    ----------
    stage : str
        The stage to get the plugin for. Can be 'sources', 'transformations', or 'destinations'.
    config : dict
        The configuration for the plugin.
    run_ts : datetime
        The timestamp when the run started.

    Returns
    -------
    StageBase
        The plugin object.

    Raises
    ------
    ValueError
        If the stage is not known.
    """
    config["run_ts"] = run_ts

    base_classes = {
        "sources": StageBase,
        "transformations": StageBase,
        "destinations": StageBase,
    }

    if stage not in base_classes:
        msg = f"Unknown stage: {stage}"
        raise ValueError(msg)

    return _get_plugin_object(f"stages.{stage}", base_classes[stage], config)


def get_handler_object(config: dict) -> logging.Handler:
    """Get a plugin object for a log handler.

    Parameters
    ----------
    config : dict
        The configuration for the log handler.

    Returns
    -------
    logging.Handler
        The log handler object.
    """
    return _get_plugin_object("log_handlers", logging.Handler, config)
