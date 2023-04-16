import json
import logging
from collections import defaultdict
from itertools import groupby
from pathlib import Path
from typing import Dict, List, Optional

import tomli
import yaml
from pydantic import (
    Field,
    FilePath,
    constr,
    dataclasses,
    root_validator,
    validate_arguments,
    validator,
)


@validate_arguments
def get_config_from_file(settings_path: Path) -> dict | None:
    """
    Load a configuration file into a dictionary. Supported formats are JSON, YAML, and TOML.

    Args:
        settings_path (Path): The path to the configuration file.

    Returns:
        dict | None: The configuration data as a dictionary, or None if an error occurred.
    """
    loaders = defaultdict(
        lambda: None, {".json": json.load, ".toml": tomli.load, ".yaml": yaml.safe_load}
    )

    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    try:
        if settings_path.exists():
            ext = settings_path.suffix
            with settings_path.open("r") as settings_file:
                return loaders[ext](settings_file)
    except FileNotFoundError:
        log.warning(f"File not found: {settings_path.as_posix()}")
    except OSError:
        log.warning(f"Could not open file: {settings_path.as_posix()}")
    except (json.JSONDecodeError, yaml.error.YAMLError, tomli.TOMLDecodeError):
        log.warning(f"File is not valid: {settings_path.as_posix()}")
    except TypeError:
        log.warning(f"Unsupported file type: {settings_path.as_posix()}")
    except RuntimeError as e:
        log.warning(f"Skipping: {settings_path.as_posix()} Encountered: {e}")


def _get_multiple_validations_with_same_rule(validations):
    """
    Group a list of validations by their rule and return only the groups that have more
    than one validation with the same rule.

    Inputs:
    - validations: a list of dictionaries containing validation rules and actions.

    Flow:
    1. The function uses the groupby function from itertools to group the validations
       by their rule.
    2. For each group, it creates a list of validation actions.
    3. It returns a dictionary with the rule as the key and the list of validation
       actions as the value, but only for groups with more than one validation.

    Outputs:
    - A dictionary with the rule as the key and a list of validation actions as the
      value for groups with more than one validation.

    Additional aspects:
    - The function uses the strip() method to remove any leading or trailing whitespace
      from the rule before grouping.
    - The function sorts the validations list by rule.
    """

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
    """
    The Validation class is designed to provide a way to define validation rules and
    actions for data. The class has two main fields: validation_rule and
    validation_action. The validation_rule field is a Spark SQL string that defines the
    rule that the input data must follow, while the validation_action field is a string
    that defines the action to take if the input data fails to meet the validation rule

    Fields:
    The Validation class has two main fields: validation_rule and validation_action.
    The validation_rule field is a string that defines the rule that the input data
    must follow. It has a minimum length of 1 character, which ensures that the input
    data is not empty. The validation_action field is a string that defines the action
    to take if the input data fails to meet the validation rule. It must be one of
    three values: LOG, DROP, or FAIL. If the input data fails to meet the validation
    rule, the action specified in this field will be taken.
    """

    validation_rule: constr(min_length=1, strict=True)
    validation_action: constr(to_upper=True, strict=True, regex=r"\A(LOG|DROP|FAIL)\Z")

    def __getitem__(self, item):
        # Without this Pydantic throws ValidationError: object is not subscriptable
        return self.__getattribute__(item)


@dataclasses.dataclass
class Source:
    """
    The Source class is designed to represent a data source and its associated
    metadata. It can handle different types of data sources, including local files,
    remote URLs, and non-empty strings. The class also allows for optional parameters
    and validations to be associated with the data source.

    Fields:
    The Source class has five main fields:
    - data: represents the data source itself, and can be of type Path, AnyUrl, or
      constr.
    - type: represents the type of data contained in the source, and is a string with a
      minimum length of 1.
    - into: represents the destination for the data, and is a string with a minimum
      length of 1.
    - params: represents optional parameters associated with the data source, and is a
      dictionary.
    - validations: optional validation rules associated with the data source. Each
      Validation object has a validation_rule field (Spark SQL) and a validation_action
      field (a string that must be either "LOG", "DROP", or "FAIL").
    """

    data: constr(min_length=1, strict=True)
    type: constr(min_length=1, strict=True)
    into: constr(min_length=1, strict=True)
    params: Optional[Dict] = Field(default_factory=dict)
    validations: Optional[List[Validation]] = Field(default_factory=list)

    @validator("validations")
    @classmethod
    def check_multiple_validations_with_same_rule(cls, value):
        """
        Validator that checks that there are no multiple validations with the same rule
        """
        if value and (fails := _get_multiple_validations_with_same_rule(value)):
            raise ValueError(f"Different actions for the same validation:\n{fails}")
        return value


@dataclasses.dataclass
class Transformation:
    """
    The Transformation class is designed to represent a data transformation with
    optional validation rules. It ensures that only one of the config or sql_query
    fields is defined and that at least one of them is defined. It also allows for a
    list of Validation objects to be included to ensure that the transformation meets
    certain criteria.

    Fields:
    - data: a required string field that represents the data view to be transformed.
    - into: a required string field that represents the desired output of the
      transformation.
    - config: an optional FilePath field that represents the configuration file to be
      used for the transformation.
    - sql_query: an optional string field that represents the SQL query to be used for
      the transformation.
    - validations: an optional list of Validation objects that represent validation
      rules to be applied to the transformation.
    """

    data: constr(min_length=1, strict=True)
    into: constr(min_length=1, strict=True)
    config: Optional[FilePath] = None
    sql_query: Optional[constr(min_length=1, strict=True)] = None
    validations: Optional[List[Validation]] = Field(default_factory=list)

    @root_validator(pre=True)
    @classmethod
    def check_only_one_of_config_or_sql_query_defined(cls, values):
        """
        Root validator method that checks that only one of the config or sql_query
        fields is defined and that at least one of them is defined.
        """
        if not any(values[v] for v in ["config", "sql_query"]):
            raise ValueError(
                "No transformation defined. Please provide either a config or a sql_query"
            )
        if all(values[t] for t in ["config", "sql_query"]):
            raise ValueError("Only one of config or sql_query allowed")
        return values

    @validator("validations")
    @classmethod
    def check_multiple_validations_with_same_rule(cls, value):
        """
        Validator that checks that there are no multiple validations with the same rule
        """
        if value and (fails := _get_multiple_validations_with_same_rule(value)):
            raise ValueError(f"Different actions for the same validation:\n{fails}")
        return value


@dataclasses.dataclass
class Destination:
    """
    Represents a Delta table destination for a batch of data. It has fields for the
    source data view, the destination table, the path to the destination, the mode of
    writing (append or upsert), keys and sequence_by for upsert mode, and optional
    validations. The class provides validation for the fields and checks that the keys
    and sequence_by fields are defined for upsert mode. It also checks that there are
    no multiple validations with the same rule.

    Fields:
    - data: a string that represents the data view to be written to the destination.
    - into: a string that represents the destination table.
    - path: an optional string that represents the file path for the destination Delta
      table. If not set, the Delta table will be managed by Databricks
    - mode: a string that represents the mode of writing (append or upsert).
    - keys: an optional list of strings that represents the keys for upsert mode.
    - sequence_by: an optional string that represents the sequence_by field for upsert
      mode.
    - validations: an optional list of Validation objects that represent validation
      rules and actions for the data.
    """

    data: constr(min_length=1, strict=True)
    into: constr(min_length=1, strict=True)
    mode: constr(min_length=1, strict=True, regex=r"^(append|upsert)$")
    path: Optional[Path] = None
    keys: Optional[List[constr(min_length=1, strict=True)]] = Field(
        default_factory=list
    )
    sequence_by: Optional[constr(min_length=1, strict=True)] = None
    validations: Optional[List[Validation]] = Field(default_factory=list)

    @root_validator(pre=True)
    @classmethod
    def check_keys_and_sequence_for_upsert(cls, values):
        """
        Root validator that checks that the keys and sequence_by fields are defined for
        upsert mode.
        """
        if values.get("mode") == "upsert" and not all(
            [values[v] for v in ["keys", "sequence_by"]]
        ):
            raise ValueError(
                "Mode upsert requires that keys and sequence_by are defined"
            )
        return values

    @validator("validations")
    @classmethod
    def check_multiple_validations_with_same_rule(cls, value):
        """
        Validator that checks that there are no multiple validations with the same rule
        """
        if value and (fails := _get_multiple_validations_with_same_rule(value)):
            raise ValueError(f"Different actions for the same validation:\n{fails}")
        return value

    @validator("path", pre=False, always=True)
    @classmethod
    def convert_to_absolute_string(cls, value: Optional[Path]) -> Optional[str]:
        """
        Validator that converts the Path object to its absolute POSIX representation
        """
        if value:
            return value.absolute().as_posix()

        return None


@dataclasses.dataclass
class Configuration:
    """
    The Configuration class is designed to represent a configuration file for a data
    pipeline. It contains optional lists of Source, Transformation, and Destination
    objects, which define the stages of the pipeline. The class provides validation to
    ensure that at least one stage is defined in the configuration file.

    Fields:
    - sources: an optional list of Source objects that represent the data sources for
      the pipeline.
    - transformations: an optional list of Transformation objects that represent the
      data transformations for the pipeline.
    - destinations: an optional list of Destination objects that represent the
      destinations for the pipeline.
    """

    sources: Optional[List[Source]] = Field(default_factory=list)
    transformations: Optional[List[Transformation]] = Field(default_factory=list)
    destinations: Optional[List[Destination]] = Field(default_factory=list)

    @root_validator(pre=True)
    @classmethod
    def check_at_least_one_stage_defined(cls, values):
        """
        Root validator method that checks that at least one of the sources,
        transformations, or destinations fields is defined in the configuration file.
        """
        if not any(v in ["sources", "transformations", "destinations"] for v in values):
            raise ValueError(
                "No stage definition found. Please define at least one of: sources, transformations, destinations"
            )
        return values
