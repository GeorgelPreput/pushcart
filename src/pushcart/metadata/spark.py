"""PySpark transformations and code generation."""

from collections.abc import Hashable
from typing import Any

import pyspark.sql.functions as F
from loguru import logger
from pyspark.sql import Column, DataFrame


def generated_col_or_transform(transformation_step: dict[Hashable, Any]) -> str:
    """Generate column or transformation function based on the transformation step.

    Parameters
    ----------
    transformation_step : dict
        Dictionary containing transformation information.

    Returns
    -------
    str
        Column name or transformation function as a string.

    """
    if (
        isinstance(transformation_step["transform_function"], str)
        and len(transformation_step["transform_function"]) > 0
    ):
        return transformation_step["transform_function"]

    return f"F.col(\"{transformation_step['source_column_name']}\")"


def generated_transform_with_default(
    transformation_step: dict[Hashable, Any],
    col_or_transf: str,
) -> str:
    """Generate transformation with default value if specified.

    Parameters
    ----------
    transformation_step : dict
        Dictionary containing transformation information.
    col_or_transf : str
        Column name or transformation function as a string.

    Returns
    -------
    str
        Column or transformation function with default value if specified.

    """
    if (
        isinstance(transformation_step["default_value"], str)
        and len(transformation_step["default_value"]) > 0
    ):
        default_value = generated_cast_or_original_dtype(
            transformation_step,
            f"F.lit(\"{transformation_step['default_value']}\")",
        )
        return f"F.coalesce({col_or_transf}, {default_value})"

    return col_or_transf


def generated_cast_or_original_dtype(
    transformation_step: dict[Hashable, Any],
    col_or_transf: str,
) -> str:
    """Cast to the destination type or return the original column based on the transformation step.

    Parameters
    ----------
    transformation_step : dict
        Dictionary containing transformation information.
    col_or_transf : str
        PySpark Column name.

    Returns
    -------
    Column
        PySpark Column name, casted or original.

    """
    if (
        transformation_step["source_column_type"]
        != transformation_step["dest_column_type"]
    ) and (
        all(
            dtype not in transformation_step["dest_column_type"]
            for dtype in ["array", "struct"]
        )
    ):
        return f"{col_or_transf}.cast(\"{transformation_step['dest_column_type']}\")"

    return col_or_transf


def generate_code(
    transformations: list[dict[Hashable, Any]],
    dest_cols: list[str],
) -> str:
    """Generate PySpark transformation code based on existing metadata.

    Parameters
    ----------
    transformations : pd.DataFrame
        Pandas DataFrame containing the transformation metadata.
    dest_cols : list of str
        List of destination column names to select in the output DataFrame.

    Returns
    -------
    str
        String containing runnable PySpark code

    """
    code_lines = ["df = (df"]

    for t in transformations:
        col = generated_col_or_transform(t)
        col = generated_transform_with_default(t, col)
        col = generated_cast_or_original_dtype(t, col)

        code_lines.append(f"\t.withColumn(\"{t['dest_column_name']}\", {col})")

    code_lines.append(f"\t.select({dest_cols}))")
    code_str = "\n" + "\n".join(code_lines)

    logger.info(code_str)

    return code_str


def col_or_transform(transformation_step: dict[Hashable, Any]) -> Column:
    """Evaluate column or transformation function based on the transformation step.

    Parameters
    ----------
    transformation_step : dict
        Dictionary containing transformation information.

    Returns
    -------
    Column
        PySpark Column object.

    """
    if (
        isinstance(transformation_step["transform_function"], str)
        and len(transformation_step["transform_function"]) > 0
    ):
        return eval(  # pylint: disable=W0123  # noqa: S307
            transformation_step["transform_function"],
        )

    return F.col(transformation_step["source_column_name"])


def transform_with_default(
    transformation_step: dict[Hashable, Any],
    col_or_transf: Column,
) -> Column:
    """Apply transformation with default value if specified.

    Parameters
    ----------
    transformation_step : dict
        Dictionary containing transformation information.
    col_or_transf : Column
        PySpark Column object.

    Returns
    -------
    Column
        PySpark Column object with default value applied if specified.

    """
    if (
        isinstance(transformation_step["default_value"], str)
        and len(transformation_step["default_value"]) > 0
    ):
        return F.coalesce(
            col_or_transf,
            cast_or_original_dtype(
                transformation_step,
                F.lit(transformation_step["default_value"]),
            ),
        )

    return col_or_transf


def cast_or_original_dtype(
    transformation_step: dict[Hashable, Any],
    col_or_transf: Column,
) -> Column:
    """Cast to the destination type or return the original column based on the transformation step.

    Parameters
    ----------
    transformation_step : dict
        Dictionary containing transformation information.
    col_or_transf : Column
        PySpark Column object.

    Returns
    -------
    Column
        PySpark Column object, casted or original.

    """
    if (
        transformation_step["source_column_type"]
        != transformation_step["dest_column_type"]
    ) and (
        all(
            dtype not in transformation_step["dest_column_type"]
            for dtype in ["array", "struct"]
        )
    ):
        return col_or_transf.cast(transformation_step["dest_column_type"])

    return col_or_transf


def transform(
    data_df: DataFrame,
    transformations: list[dict[Hashable, Any]],
    dest_cols: list[str],
) -> DataFrame:
    """Perform the transformations configured in the metadata table on the input DataFrame.

    Parameters
    ----------
    data_df : DataFrame
        Input PySpark DataFrame.
    transformations : pd.DataFrame
        Pandas DataFrame containing the transformation metadata.
    dest_cols : list of str
        List of destination column names to select in the output DataFrame.

    Returns
    -------
    DataFrame
        PySpark DataFrame containing the transformed data.

    """
    result_df = data_df
    for t in transformations:
        col = col_or_transform(t)
        col = transform_with_default(t, col)
        col = cast_or_original_dtype(t, col)

        result_df = result_df.withColumn(t["dest_column_name"], col)

    return result_df.select(dest_cols)
