"""Functions supporting splitting out nested columns into their own separate table."""
import pyspark.sql.functions as F  # noqa: N812
import pyspark.sql.types as T  # noqa: N812
from loguru import logger
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()


# TODO: This handles arrays rather badly for now
def _is_top_level_col(col_name: str) -> bool:
    """Check if a column is a top-level column.

    Parameters
    ----------
    col_name : str
        Name of the column.

    Returns
    -------
    bool
        True if the column is top-level, False otherwise.
    """
    return "." not in col_name


def _is_array(df: DataFrame, col_name: str) -> bool:
    """Check if a column is of array type.

    Parameters
    ----------
    df : DataFrame
        Input PySpark DataFrame.
    col_name : str
        Name of the column.

    Returns
    -------
    bool
        True if the column is of array type, False otherwise.
    """
    col_dtype = T._parse_datatype_string(  # noqa: SLF001
        df.select(col_name).dtypes[0][1],
    )

    if isinstance(col_dtype, T.ArrayType):
        return True

    return False


def _is_struct(df: DataFrame, col_name: str) -> bool:
    """Check if a column is of struct type.

    Parameters
    ----------
    df : DataFrame
        Input PySpark DataFrame.
    col_name : str
        Name of the column.

    Returns
    -------
    bool
        True if the column is of struct type, False otherwise.
    """
    col_dtype = T._parse_datatype_string(  # noqa: SLF001
        df.select(col_name).dtypes[0][1],
    )

    if isinstance(col_dtype, T.StructType):
        return True

    return False


def _is_complex_type(df: DataFrame, col_name: str) -> bool:
    """Check if a column is of complex type (array or struct).

    Parameters
    ----------
    df : DataFrame
        Input PySpark DataFrame.
    col_name : str
        Name of the column.

    Returns
    -------
    bool
        True if the column is of complex type, False otherwise.
    """
    if _is_array(df, col_name) or _is_struct(df, col_name):
        return True

    return False


def _separate_parent_from_child(col_name: str) -> tuple[str, str]:
    """Separate the parent column name from the child column name.

    Parameters
    ----------
    col_name : str
        Name of the column.

    Returns
    -------
    tuple[str, str]
        A tuple containing the parent column name and the child column name.
    """
    if "." not in col_name:
        return (None, col_name)

    return (col_name[: col_name.rindex(".")], col_name[col_name.rindex(".") + 1 :])


def _hash_top_level_col(df: DataFrame, col_name: str) -> DataFrame:
    """Add a hash key column to the top-level column.

    Parameters
    ----------
    df : DataFrame
        Input PySpark DataFrame.
    col_name : str
        Name of the column.

    Returns
    -------
    DataFrame
        Updated DataFrame with a new hash key column.
    """
    if _is_complex_type(df, col_name):
        return df.withColumn(f"{col_name}_key", F.sha1(F.to_json(F.col(col_name))))

    return df.withColumn(f"{col_name}_key", F.sha1(F.col(col_name)))


def _hash_nested_col(df: DataFrame, col_name: str) -> DataFrame:
    """Add a hash key column to the nested column.

    Parameters
    ----------
    df : DataFrame
        Input PySpark DataFrame.
    col_name : str
        Name of the column.

    Returns
    -------
    DataFrame
        Updated DataFrame with a new hash key column.
    """
    parent_col, child_col = _separate_parent_from_child(col_name)

    if _is_complex_type(df, col_name):
        return df.withColumn(
            parent_col,
            F.struct(
                *[
                    F.col(f"{parent_col}.*"),
                    F.sha1(F.to_json(F.col(col_name))).alias(f"{child_col}_key"),
                ],
            ),
        )

    return df.withColumn(
        parent_col,
        F.struct(
            *[
                F.col(f"{parent_col}.*"),
                F.sha1(F.col(col_name)).alias(f"{child_col}_key"),
            ],
        ),
    )


def _get_fields_to_drop(col_name: str, processed_cols: list[str]) -> list[str]:
    """Get the fields that need to be dropped.

    Parameters
    ----------
    col_name : str
        Name of the column.
    processed_cols : list[str]
        List of processed column names.

    Returns
    -------
    list[str]
        List of fields to drop.
    """
    if not processed_cols:
        return []

    if col_name in processed_cols:
        processed_cols.remove(col_name)

    return [col.split(f"{col_name}.")[-1] for col in processed_cols if col_name in col]


def _drop_processed_cols(
    input_df: DataFrame,
    col_name: str,
    fields_to_drop: list = None,
) -> DataFrame:
    """Drop the processed columns from the DataFrame.

    Parameters
    ----------
    input_df : DataFrame
        Input PySpark DataFrame.
    col_name : str
        Name of the column.
    fields_to_drop : list, optional
        List of fields to drop from the column.

    Returns
    -------
    DataFrame
        Updated DataFrame with the processed columns removed.
    """
    if fields_to_drop:
        input_df = input_df.withColumn(
            col_name,
            F.col(col_name).dropFields(*fields_to_drop),
        )

    return input_df


def _create_split_table(
    input_df: DataFrame,
    col_name: str,
    fields_to_drop: list = None,
) -> DataFrame:
    """Create a new table by splitting a column from the original DataFrame.

    Parameters
    ----------
    input_df : DataFrame
        Input PySpark DataFrame.
    col_name : str
        Name of the column to split.
    fields_to_drop : list, optional
        List of fields to drop from the column.

    Returns
    -------
    DataFrame
        New DataFrame containing the split column.
    """
    col_name_flat = col_name.replace(".", "__")

    logger.info(f"Splitting column {col_name_flat} from original dataframe.")
    output_df = _drop_processed_cols(input_df, col_name, fields_to_drop or [])

    if _is_array(output_df, col_name):
        return output_df.select(
            F.col(f"{col_name}_key").alias(f"{col_name_flat}_key"),
            F.posexplode(col_name).alias(
                f"{col_name_flat}_pos",
                f"{col_name_flat}_col",
            ),
            *[F.col(f"{processed_col}") for processed_col in fields_to_drop],
        )

    output_df = output_df.select(
        F.col(f"{col_name}_key").alias(f"{col_name_flat}_key"),
        F.col(f"{col_name}.*"),
    )

    return output_df.distinct()


def _split_root(input_df: DataFrame, cols: list[str]) -> DataFrame:
    """Prepare the root DataFrame by hashing specified columns.

    This function processes the input DataFrame by hashing the specified columns,
    in preparation for splitting those columns into separate DataFrames.

    Parameters
    ----------
    input_df : DataFrame
        Input PySpark DataFrame.
    cols : list[str]
        List of column names that need to be hashed. These are sorted based on
        the number of "." characters, in decreasing order.

    Returns
    -------
    DataFrame
        The updated DataFrame, with hash columns added for each of the specified columns.
    """
    cols = sorted(cols, key=lambda x: x.count("."), reverse=True)

    logger.info("Creating dataframe to serve as base for splitting.")

    for col_name in cols:
        if _is_top_level_col(col_name):
            input_df = _hash_top_level_col(input_df, col_name)
        else:
            input_df = _hash_nested_col(input_df, col_name)

    return input_df


def _split_children(root_df: DataFrame, cols: list[str]) -> DataFrame:
    """Split specified columns from the root DataFrame into separate DataFrames.

    For each specified column, this function creates a new DataFrame by splitting
    that column from the root DataFrame. The new DataFrames are stored in a dictionary
    where each key is a column name and each value is the corresponding split DataFrame.

    Parameters
    ----------
    root_df : DataFrame
        The root PySpark DataFrame, which serves as the base for splitting.
    cols : list[str]
        List of column names to be split. These are sorted based on the number of
        "." characters, in decreasing order.

    Returns
    -------
    dict[str, DataFrame]
        Dictionary where each key is a column name, and each value is a new DataFrame
        containing that column.
    """
    cols = sorted(cols, key=lambda x: x.count("."), reverse=True)

    processed_cols = []
    children = {}

    for col_name in cols:
        fields_to_drop = _get_fields_to_drop(col_name, processed_cols)
        split_table = _create_split_table(root_df, col_name, fields_to_drop)

        logger.info(f"Created split dataframe for {col_name} column.")

        children.update({col_name: split_table})
        processed_cols.append(col_name)

    return children


def split(input_df: DataFrame, cols: list[str]) -> dict[str, DataFrame]:
    """Split specified columns into separate DataFrames.

    Parameters
    ----------
    input_df : DataFrame
        Input PySpark DataFrame.
    cols : list[str]
        List of column names to be split.

    Returns
    -------
    dict[str, DataFrame]
        Dictionary where each key is a column name (with "__" replacing ".")
        and each value is a new DataFrame containing that column.
    """
    root_df = _split_root(input_df, cols)
    children = _split_children(root_df, cols)

    dataframes = {
        "root": root_df.drop(*cols),
    }
    dataframes.update(children)

    return dataframes
