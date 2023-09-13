"""Function to flatten nested PySpark DataFrames using DLT."""
# pylint: disable=import-error
# pyright: reportMissingImports=false
import dlt  # type: ignore[import]
from pyspark.sql import DataFrame

import pushcart.transformations.flatten_nested.spark as ps


def flatten(source: str, target: str, streaming: bool = False) -> None:
    """Read a source table, flatten it, and write to a target table.

    Parameters
    ----------
    source : str
        Name of the source table to read from.
    target : str
        Name of the target table to write to.
    streaming : bool, optional
        If True, read the source table as a stream. Default is False.
    """
    @dlt.table(name=target, temporary=True)
    def flatten_in_place(source_table: str = source) -> DataFrame:
        nested_df = dlt.read_stream(source_table) if streaming else dlt.read(source_table)

        return ps.flatten(nested_df)
