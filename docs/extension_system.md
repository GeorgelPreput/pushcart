# Pushcart Extension System

The Pushcart extension system allows you to create and integrate custom sources, transformations, and destinations as modular components. This makes it easy to extend the core functionality of Pushcart with independently developed packages while keeping the main library lightweight and focused.

## Overview

Pushcart's extension system is built around the concept of stages. A stage can be a **source**, **transformation**, or **destination**. The core library provides base classes for each stage:

- `SourceBase`
- `TransformationBase`
- `DestinationBase`

These base classes should be subclassed when creating custom extensions for each stage. The extension system takes care of discovering and loading the custom stage classes at runtime.

## Developing an Extension

To create a custom extension, follow these general steps:

1. Create a new Python package with a name that follows the template `pushcart-<stage_name>`. For example, `pushcart-rest-api`, `pushcart-sql-server`, or `pushcart-dataset`.

1. Inside the package, create a module for each custom stage class. The module name should be lowercase and match the desired configuration key. For example, create a module `sources/rest_api.py` for a `RestApiSource` class.

1. In the module, create a class that subclasses the appropriate base class (e.g., `SourceBase`, `TransformationBase`, or `DestinationBase`). The class name should follow the convention `ModuleNameStageName`. For example, `RestApiSource`, `SqlServerDestination`, or `DatasetTransformation`.

1. Implement the required methods and any optional methods specific to your custom stage.

1. Add an `__init__.py` file in the package and stage folders, and include the custom stage class in the `__all__` list.

1. Install the custom package in the same environment as Pushcart.

## Extension Discovery

Pushcart uses an autodiscovery mechanism to find and load custom stage classes. The discovery process searches for installed packages with names that match the following regular expression pattern:

```regex
pushcart(?:_?-?[a-z]+)*
```

This pattern will match packages like `pushcart-rest-api`, `pushcart-sql-server`, and `pushcart-dataset`.

The discovery process then scans the package for modules and classes that subclass the appropriate base class for the stage (e.g., `SourceBase`, `TransformationBase`, or `DestinationBase`). Circular imports and submodule name conflicts are ignored.

## Packaging and Distributing Extensions as Extras

You can package your custom extension as an extra for the Pushcart package. This allows users to easily install your extension alongside Pushcart. To add your extension as an extra, include it in the `pyproject.toml` file of the Pushcart package under `[tool.poetry.extras]`.

For example, if you have a custom extension called `pushcart_rest_api`, you can add it as an extra like this:

```toml
[tool.poetry.extras]
rest_api = ["pushcart_rest_api"]
```

Now users can install Pushcart along with the `rest_api` extension using the following command:

```bash
pip install pushcart[rest_api]
```

This command will install both Pushcart and the `pushcart_rest_api` extension in the user's environment. Make sure to properly document the available extras and their functionalities for users to easily understand which extension suits their needs.

## Using Custom Stages

To use a custom stage in a Pushcart workflow, simply specify the configuration key that matches the module name in the workflow configuration. For example, if you have a custom source class named `RestApiSource` in the `pushcart_rest_api.sources.rest_api` module, use the key `rest_api` in the source configuration:

```yaml
sources:
  - type: rest_api
    # other configuration options specific to RestApiSource
```

Pushcart will automatically discover and instantiate the custom stage class with the provided configuration.

## Caching

To optimize performance, the extension discovery system uses the `@lru_cache` decorator to cache the results of package and class discovery. This ensures that the discovery process is performed only once per stage, even if the same stage is used multiple times in a workflow.
