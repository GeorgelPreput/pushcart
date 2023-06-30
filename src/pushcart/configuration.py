"""Data pipeline configuration.

Metadata configuration for running a data pipeline using Pushcart. Is instantiated
off of a Python dictionary, or a JSON, TOML or YAML file, optionally in conjuction
with a CSV file for transformation definitions.

Example:
-------
    config_dict = json.load(file)
    pipeline_config = Configuration.parse_obj(config_dict)

Notes:
-----
Configuration must have at least one stage (Source, Transformation, Destination) of the
data pipeline defined.

"""

from itertools import groupby
from pathlib import Path

from pydantic import Field, conint, constr, dataclasses, root_validator, validator


def _get_multiple_validations_with_same_rule(validations: dict) -> dict:
    """Group a list of validations by their rule and return only the groups that have more than one validation with the same rule."""
    validation_groups = {
        k: [v["validation_action"] for v in v]
        for k, v in groupby(
            sorted(validations, key=lambda v: str(v["validation_rule"]).strip()),
            lambda r: str(r["validation_rule"]).strip(),
        )
    }

    return {k: v for k, v in validation_groups.items() if len(v) > 1}


@dataclasses.dataclass
class Validation:
    """Provides a way to define validation rules and actions for data.

    Has two main fields: validation_rule and validation_action. The validation_rule
    field is a Spark SQL string that defines the rule that the input data must follow,
    while the validation_action field is a string that defines the action to take if
    the input data fails to meet the validation rule.
    """

    validation_rule: constr(min_length=1, strict=True)
    validation_action: constr(to_upper=True, strict=True, regex=r"\A(LOG|DROP|FAIL)\Z")

    def __getitem__(self, item: str) -> any:
        """Avoid Pydantic throwing ValidationError: object not subscriptable.

        Parameters
        ----------
        item : str
            Name of parent object attribute

        Returns
        -------
        any
            Type of returned object
        """
        return self.__getattribute__(item)


@dataclasses.dataclass
class Source:
    """Represents a data source and its associated metadata.

    Handles different types of data sources, including local files, remote URLs, and
    non-empty strings. The class also allows for optional parameters and validations
    to be associated with the data source.

    Returns
    -------
    Source
        Object defining a Pipeline data source.

    Raises
    ------
    ValueError
        Only one action (WARN | DROP | FAIL) can be defined as consequence to a data
        validation rule
    """

    target_catalog_name: constr(strip_whitespace=True, min_length=1, strict=True)
    target_schema_name: constr(strip_whitespace=True, min_length=1, strict=True)
    pipeline_name: constr(strip_whitespace=True, min_length=1, strict=True)

    origin: constr(min_length=1, strict=True)
    datatype: constr(min_length=1, strict=True)
    target: constr(min_length=1, strict=True)
    params: str | None = None
    validations: list[Validation] | None = Field(default_factory=list)

    @validator("validations")
    @classmethod
    def check_multiple_validations_with_same_rule(cls, value: dict) -> dict:
        """Check that there are no multiple validation actions for the same rule."""
        if value and (fails := _get_multiple_validations_with_same_rule(value)):
            msg = f"Different actions for the same validation:\n{fails}"
            raise ValueError(msg)
        return value


@dataclasses.dataclass
class Transformation:
    """Represents a data transformation with optional validation rules.

    It ensures that only one of the config or sql_query fields is defined and that at
    least one of them is defined. It also allows for a list of Validation objects to
    be included to ensure that the transformed data meets desired criteria.

    Returns
    -------
    Transformation
        Object defining a transformation step within a data Pipeline.

    Raises
    ------
    ValueError
        Transformation needs to be based on a SQL query or a metadata .csv file
    ValueError
        Transformation must only have one of either a SQL query or a metadata .csv file
    ValueError
        Only one action (WARN | DROP | FAIL) can be defined as consequence to a data
        validation rule
    """

    target_catalog_name: constr(strip_whitespace=True, min_length=1, strict=True)
    target_schema_name: constr(strip_whitespace=True, min_length=1, strict=True)
    pipeline_name: constr(strip_whitespace=True, min_length=1, strict=True)

    origin: constr(min_length=1, strict=True)
    target: constr(min_length=1, strict=True)
    column_order: conint(ge=1) | None = 1
    source_column_name: constr(strict=True) | None = None
    source_column_type: constr(
        strict=True,
        regex="\\A(string|int|double|date|timestamp|boolean|struct|array|map)\\Z",
    ) | None = None
    dest_column_name: constr(strict=True) | None = None
    dest_column_type: constr(
        strict=True,
        regex="\\A(string|int|double|date|timestamp|boolean|struct|array|map)\\Z",
    ) | None = None
    transform_function: constr(strict=True) | None = None
    sql_query: constr(min_length=1, strict=True) | None = None
    default_value: constr(strict=True) | None = None
    validations: list[Validation] | None = Field(default_factory=list)

    @root_validator(pre=True)
    @classmethod
    def check_only_one_of_config_or_sql_query_defined(cls, values: dict) -> dict:
        """Check that one and only one of the config or sql_query fields is defined."""
        if not any(
            values.get(v) is not None
            for v in ["source_column_name", "dest_column_name", "sql_query"]
        ):
            msg = f"No transformation defined. Please provide either a config or a sql_query.\nGot: {values}"
            raise ValueError(msg)
        if all(
            values.get(t)
            for t in ["source_column_name", "dest_column_name", "sql_query"]
        ):
            msg = f"Only one of config or sql_query allowed.\nGot: {values}"
            raise ValueError(msg)
        return values

    @validator("validations")
    @classmethod
    def check_multiple_validations_with_same_rule(cls, value: dict) -> dict:
        """Validate that there are no multiple validations with the same rule."""
        if value and (fails := _get_multiple_validations_with_same_rule(value)):
            msg = f"Different actions for the same validation:\n{fails}"
            raise ValueError(msg)
        return value


@dataclasses.dataclass
class Destination:
    """Represents a Delta table destination for a batch of data.

    Defines fields for the source data view, the destination table, the path to the
    destination, the mode of writing (append or upsert), keys and sequence_by for
    upsert mode, and optional validations. Provides validation for the fields and
    checks that the keys and sequence_by fields are defined for upsert mode. Checks
    that there are no multiple validations with the same rule.

    Returns
    -------
    Destination
        Object defining a destination for the data Pipeline to write to.

    Raises
    ------
    ValueError
        When upserting to a destination, the primary key and sequence columns must be
        defined.
    ValueError
        Only one action (WARN | DROP | FAIL) can be defined as consequence to a data
        validation rule
    """

    target_catalog_name: constr(strip_whitespace=True, min_length=1, strict=True)
    target_schema_name: constr(strip_whitespace=True, min_length=1, strict=True)
    pipeline_name: constr(strip_whitespace=True, min_length=1, strict=True)

    origin: constr(min_length=1, strict=True)
    target: constr(min_length=1, strict=True)
    mode: constr(min_length=1, strict=True, regex=r"^(append|upsert)$")
    path: Path | None = None
    keys: list[constr(min_length=1, strict=True)] | None = Field(
        default_factory=list,
    )
    sequence_by: constr(min_length=1, strict=True) | None = None
    validations: list[Validation] | None = Field(default_factory=list)

    @root_validator(pre=True)
    @classmethod
    def check_keys_and_sequence_for_upsert(cls, values: dict) -> dict:
        """Check that the keys and sequence_by fields are defined for upsert mode."""
        if values.get("mode") == "upsert" and not all(
            values[v] for v in ["keys", "sequence_by"]
        ):
            msg = "Mode upsert requires that keys and sequence_by are defined"
            raise ValueError(
                msg,
            )
        return values

    @validator("validations")
    @classmethod
    def check_multiple_validations_with_same_rule(cls, value: dict) -> dict:
        """Check that there are no multiple validations with the same rule."""
        if value and (fails := _get_multiple_validations_with_same_rule(value)):
            msg = f"Different actions for the same validation:\n{fails}"
            raise ValueError(msg)
        return value

    @validator("path", pre=False, always=True)
    @classmethod
    def convert_to_absolute_string(cls, value: Path | None) -> str | None:
        """Convert the Path object to its absolute POSIX representation."""
        if value:
            return value.absolute().as_posix()

        return None


@dataclasses.dataclass
class Configuration:
    """Represents a configuration file for a data pipeline.

    Returns
    -------
    Configuration
        Contains optional lists of Source, Transformation, and Destination objects,
        which define the stages of the pipeline. Provides validation to ensure that
        at least one stage is defined in the configuration file.

    Raises
    ------
    ValueError
        At least one stage definition (Source, Transformation, Destination) must exist.
    ValueError
        All values in the "target" fields of all pipeline stages taken together must be
        unique.
    """

    sources: list[Source] | None = Field(default_factory=list)
    transformations: list[Transformation] | None = Field(default_factory=list)
    destinations: list[Destination] | None = Field(default_factory=list)

    @root_validator(pre=True)
    @classmethod
    def check_at_least_one_stage_defined(cls, values: dict) -> dict:
        """Check that at least one of the stages is defined in the configuration file."""
        if not any(v in ["sources", "transformations", "destinations"] for v in values):
            msg = "No stage definition found. Please define at least one of: sources, transformations, destinations"
            raise ValueError(msg)
        return values

    @root_validator(pre=True)
    @classmethod
    def check_all_dlt_target_objects_are_unique(cls, values: dict) -> dict:
        """Check that no values of the stage "target" fields, taken together, overlap."""
        sources = values.get("sources", [])
        transformations = values.get("transformations", [])
        destinations = values.get("destinations", [])

        target_values = (
            [source.get("target") for source in sources if source]
            + [
                transformation.get("target")
                for transformation in transformations
                if transformation.get("sql_query")
            ]
            + [destination.get("target") for destination in destinations if destination]
        )

        duplicates = [
            value for value in set(target_values) if target_values.count(value) > 1
        ]

        if duplicates:
            msg = f"Duplicate 'target' values found: {', '.join(duplicates)}"
            raise ValueError(msg)

        return values
