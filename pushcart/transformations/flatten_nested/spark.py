"""Functions to flatten nested structures in PySpark DataFrames."""
from typing import Union

import pyspark.sql.functions as F  # noqa: N812
import pyspark.sql.types as T  # noqa: N812
from pyspark.sql import DataFrame


def _flatten_struct(schema: T.StructType, prefix: Union[str, None]=None) -> list[str]:
    """Recursively flatten nested StructType fields.

    Parameters
    ----------
    schema : T.StructType
        The PySpark StructType schema to flatten.
    prefix : Union[str, None], optional
        A prefix to prepend to field names, by default None.


    Returns
    -------
    list[str]
        A list of flattened field names.
    """
    fields = []
    for field in schema.fields:
        name = prefix + "." + field.name if prefix else field.name
        if isinstance(field.dataType, T.StructType):
            fields += _flatten_struct(field.dataType, prefix=name)
        else:
            fields.append(name)

    return fields


def _get_array_columns(schema: T.StructType) -> list[str]:
    """Return names of all columns of type ArrayType in a given schema.

    Parameters
    ----------
    schema : T.StructType
        The PySpark StructType schema to inspect.


    Returns
    -------
    list[str]
        A list of field names of type ArrayType.
    """
    return [
        field.name for field in schema.fields if isinstance(field.dataType, T.ArrayType)
    ]


def flatten(nested_df: DataFrame) -> DataFrame:
    """Flatten a nested PySpark DataFrame.

    This function will also explode array-type columns and flatten the resultant structure.


    Parameters
    ----------
    nested_df : DataFrame
        The PySpark DataFrame to flatten.


    Returns
    -------
    DataFrame
        A flattened version of the input DataFrame.
    """
    array_columns = _get_array_columns(nested_df.schema)

    nested_df = nested_df.select(
        *[
            F.col(col_name).alias(col_name.replace(".", "__"))
            for col_name in _flatten_struct(nested_df.schema)
        ],
        *[
            F.posexplode_outer(col_name).alias(
                f"{col_name.replace('.', '__')}_pos",
                f"{col_name.replace('.', '__')}_col",
            )
            for col_name in array_columns
        ],
    ).drop(*array_columns)

    if any(
        isinstance(field.dataType, (T.StructType, T.ArrayType))
            for field in nested_df.schema.fields
    ):
        return flatten(nested_df)

    return nested_df
