"""Create DLT objects splitting nested columns into separate tables."""

# pylint: disable=import-error
# pyright: reportMissingImports=false
import dlt
from pyspark.sql import DataFrame

import pushcart.transformations.split_nested.spark as ps


def split(source: str, target: str, cols: list[str]) -> None:
    """Split specified columns from a source table into separate Databricks Delta Live Tables (DLTs).

    This function reads data from a specified source table, splits the specified columns into
    separate PySpark DataFrames, and then creates Databricks Delta Live Tables (DLTs) for each
    of these DataFrames. The main table is stored with the name specified by the 'target' parameter,
    and the split tables are stored with names that are extensions of this main table name.

    Parameters
    ----------
    source : str
        The name of the source table to read the data from.
    target : str
        The name of the target DLT that will store the main table.
    cols : list[str]
        List of column names to be split into separate tables.

    Notes
    -----
    This function does not return any value; its primary purpose is the side effect of creating DLT objects.
    """
    input_df = dlt.read(source)
    split_dfs = ps.split(input_df, cols)

    @dlt.table(name=target, temporary=True)
    def root_table(dataframes: dict[str, DataFrame] = split_dfs):  # noqa: ANN202
        return dataframes["root"]

    for col_name in cols:

        @dlt.table(name=f"{target}__{col_name}", temporary=True)
        def split_view(  # noqa: ANN202
            dataframes: dict[str, DataFrame] = split_dfs,
            col: str = col_name,
        ):
            return dataframes[col]
