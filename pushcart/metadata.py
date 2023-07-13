"""Generate and edit metadata based on an input Spark DataFrame."""

from __future__ import annotations

import contextlib
import logging

import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pyspark.sql.types as T  # noqa: N812
from dateutil.parser import ParserError
from ipydatagrid import DataGrid
from IPython.display import display
from pandas.core.tools.datetimes import _guess_datetime_format_for_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from pushcart.utils import multireplace

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def __get_spark_session() -> SparkSession:
    return SparkSession.getActiveSession() or SparkSession.newSession()


def _infer_json_schema(df: DataFrame, column_name: str) -> str:
    log.info(f"Attempting to infer JSON schema for {column_name} column.")

    spark = __get_spark_session()
    schema = spark.read.json(df.select(column_name).rdd.map(lambda x: x[0])).schema

    if schema.fieldNames() == ["_corrupt_record"]:
        log.warning(f"Could not infer JSON schema for {column_name} column.")
        return None

    if "_corrupt_record" in schema.fieldNames():
        schema.fields.pop(
            schema.fields.index(
                T.StructField("_corrupt_record", T.StringType(), nullable=True),
            ),
        )

    return f'F.from_json(F.col("{column_name}"), schema="{schema.simpleString()}")'


def _pandas_to_spark_datetime_pattern(pattern: str) -> str:
    replacement_map = {
        "%Y": "yyyy",
        "%y": "yy",
        "%m": "MM",
        "%B": "MMMM",
        "%b": "MMM",
        "%d": "dd",
        "%A": "EEEE",
        "%a": "EEE",
        "%H": "HH",
        "%I": "hh",
        "%p": "a",
        "%M": "mm",
        "%S": "ss",
        "%f": "SSSSSS",
        "%z": "Z",
        "%Z": "z",
        "T": "'T'",
    }

    return multireplace(pattern, replacement_map)


def _infer_timestamps(df: DataFrame, column_name: str) -> str:
    log.info(f"Attempting to infer timestamp format for {column_name} column.")

    pd_ts_format = None

    with contextlib.suppress(ParserError, AttributeError, TypeError):
        pd_ts_df = df.select(column_name).dropna().limit(1).toPandas()
        # Pandas keeps the last bit of the field path as column name
        pd_ts_list = pd_ts_df[column_name.split(".")[-1]].to_numpy()

        pd_ts_format = _guess_datetime_format_for_array(pd_ts_list)

    if not pd_ts_format:
        log.warning(f"Could not infer timestamp format for {column_name} column.")
        return None

    spark_ts_format = _pandas_to_spark_datetime_pattern(pd_ts_format)

    return f'F.to_timestamp(F.col("{column_name}"), "{spark_ts_format}")'


def get_unique_values(df: DataFrame, max_rows: int = None) -> DataFrame:
    """Get a dataframe with only per-column unique values for inference.

    Useful for more accurately inferring metadata for some datasets.
    Can take a long time to run, but will also reduce the runtime for
    `Metadata` generation if used as its input.

    This method will break the inter-column consistency of records, since
    the distinct() operation is performed per each column. For example:

    ```
    [
        { "a": 1, "b": 5 },
        { "a": 1, "b": 6 },
        { "a": 2, "b": 7 },
        { "a": 3, "b": 7 },
        { "a": 4, "b": 7 }
    ]
    ```

    Will yield:

    ```
    [
        { "a": 1, "b": 5 },
        { "a": 2, "b": 6 },
        { "a": 3, "b": 7 },
        { "a": 4, "b": None }
    ]
    ```

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame containing data to be analyzed
    max_rows : int, optional
        Number of unique values to bring in per column, by default None.
        If not specified, max_rows will use the total record count of `df`

    Returns
    -------
    DataFrame
        Spark DataFrame containing per-column unique data
    """
    if not df:
        return None

    spark = __get_spark_session()
    if df.isEmpty():
        return spark.createDataFrame([], df.schema)

    if not max_rows:
        max_rows - df.count()

    unique_vals_df: DataFrame = spark.createDataFrame(
        [{"_row_index": r} for r in range(1, max_rows)],
    )

    for c in df.columns:
        unique_vals_df = unique_vals_df.join(
            df.select(c)
            .distinct()
            .limit(max_rows)
            .withColumn(
                "_row_index",
                F.row_number()
                .over(
                    Window.orderBy(F.monotonically_increasing_id()),
                )
                .alias(f"{c}_col"),
            ),
            on=["_row_index"],
            how="left_outer",
        )

    return unique_vals_df.select(
        [col for col in unique_vals_df.columns if not col.startswith("_")],
    )


class Metadata:
    """Generate metadata and transformations based on input DataFrame."""

    def __init__(
        self,
        df: DataFrame,
        infer_json_schema: bool = True,
        infer_timestamps: bool = True,
        infer_fraction: float = 0.25,
    ) -> None:
        """Generate metadata and transformations based on input DataFrame.

        Parameters
        ----------
        df : DataFrame
            PySpark DataFrame for which metadata is generated
        infer_json_schema : bool, optional
            Whether to infer the schema of JSON string columns or not, by default True
        infer_timestamps : bool, optional
            Whether to try to infer timestamp string column formats, by default True
        infer_fraction : float, optional
            The fraction of data that is sampled from the input, by default 0.25
        """
        self.data_df = df

        self.infer_json_schema = infer_json_schema
        self.infer_timestamps = infer_timestamps
        self.infer_fraction = infer_fraction

        self.metadata_df: pd.DataFrame = None
        self.metadata_grid: DataGrid = None

        self.metadata_cols = [
            "column_order",
            "source_column_name",
            "source_column_type",
            "dest_column_name",
            "dest_column_type",
            "transform_function",
            "default_value",
            "validation_rule",
            "validation_action",
        ]

    @classmethod
    def from_existing_csv(cls, path: str) -> Metadata:
        """Load metadata from an existing metadata file, without the original dataset.

        Parameters
        ----------
        path : str
            Path to metadata file.

        Returns
        -------
        Metadata
            Object allowing to visualize, edit, and generate PySpark code from metadata.
        """
        md = Metadata(df=None)
        md.metadata_df = pd.read_csv(path)

        return md

    def _infer(self, column_name: str) -> str:
        sampled_df = self.data_df.sample(
            fraction=self.infer_fraction,
            withReplacement=False,
        )

        inferred_ts = None
        inferred_schema = None

        if self.infer_timestamps:
            try:
                inferred_ts = _infer_timestamps(sampled_df, column_name)
                if inferred_ts:
                    return inferred_ts
            except Exception:
                log.exception(
                    f"Error while inferring timestamp format for {column_name} column.",
                )

        if self.infer_json_schema:
            try:
                inferred_schema = _infer_json_schema(sampled_df, column_name)
                if inferred_schema:
                    return inferred_schema
            except Exception:
                log.exception(
                    f"Error while inferring JSON schema for {column_name} column.",
                )

        return ""

    def _generate_field(self, field: T.StructField, parent: str = None) -> dict:
        name = field.name
        dtype = field.dataType
        flat_parent = parent.replace(".", "_") if parent else None
        return {
            "source_column_name": f"{flat_parent}.{name}" if parent else name,
            "source_column_type": dtype.simpleString(),
            "dest_column_name": f"{flat_parent}_{name}" if parent else name,
            "dest_column_type": dtype.elementType.simpleString()
            if isinstance(dtype, T.ArrayType)
            else dtype.simpleString(),
            "transform_function": f'F.explode("{flat_parent}.{name}")'
            if isinstance(dtype, T.ArrayType)
            else self._infer(f"{parent}.{name}" if parent else name)
            if isinstance(dtype, T.StringType)
            else "",
            "default_value": "",
            "validation_rule": "",
            "validation_action": "",
        }

    def _generate(self, schema: T.StructType, parent: str = None) -> list:
        fields = []

        for f in schema.fields:
            dtype = f.dataType

            field_name = f"{parent + '.' if parent else ''}{f.name}"

            if isinstance(dtype, T.ArrayType) and isinstance(
                dtype.elementType,
                T.StructType,
            ):
                fields.append(self._generate_field(f, parent))
                fields += self._generate(
                    dtype.elementType,
                    parent=field_name,
                )
            elif isinstance(dtype, T.StructType):
                fields += self._generate(dtype, parent=field_name)
            else:
                fields.append(self._generate_field(f, parent))

        return fields

    def generate(self) -> pd.DataFrame:
        """Generate a Pandas DataFrame containing the metadata for the dataset being analyzed.

        Returns
        -------
        pd.DataFrame
            Pandas DataFrame containing proposed transformation metadata
        """
        if self.data_df is None:
            msg = "Cannot generate new metadata since no Spark DataFrame has been provided."
            raise ValueError(msg)

        metadata = pd.DataFrame(
            self._generate(self.data_df.schema, parent=None),
            columns=self.metadata_cols,
        )

        metadata["column_order"] = metadata.index

        return metadata

    def _on_cell_changed(self, _: dict) -> None:
        self.metadata_df = self.metadata_grid.data

    def visualize(self) -> None:
        """Show the generated metadata and allow user editing.

        Raises
        ------
        ValueError
            Can only show the metadata if it is present in memory.
        """
        if self.metadata_df is None:
            msg = "Cannot visualize metadata as it has not yet been generated/loaded."
            raise ValueError(msg)

        self.metadata_grid = DataGrid(self.metadata_df, editable=True)
        self.metadata_grid.auto_fit_params = {"area": "all"}
        self.metadata_grid.auto_fit_columns = True
        self.metadata_grid.on_cell_change(self._on_cell_changed)
        display(self.metadata_grid)

    def get_metadata(self) -> None:
        """Generate new metadata for the provided dataset and visualize it."""
        self.metadata_df = self.generate()
        self.visualize()

    def save_to_csv(self, path: str) -> None:
        """Save the current version of the metadata to a CSV file.

        Parameters
        ----------
        path : str
            File path to save to
        """
        self.metadata_df.to_csv(path)
        log.info(f"Wrote {len(self.metadata_df.index)} lines to {path}")

    @staticmethod
    def _keep_dest_cols(metadata_df: pd.DataFrame) -> pd.DataFrame:
        has_dest_col = (metadata_df["dest_column_name"].isna()) | (
            metadata_df["dest_column_name"] == ""  # noqa: PLC1901
        )
        return metadata_df.loc[~has_dest_col]

    @staticmethod
    def _drop_technical_cols(metadata_df: pd.DataFrame) -> pd.DataFrame:
        tech_cols = metadata_df["dest_column_name"].str.startswith("_")

        excluded_columns = (
            metadata_df.loc[tech_cols]["dest_column_name"].to_list() or None
        )
        log.info(f"Excluding technical columns: {excluded_columns}")

        return metadata_df.loc[~tech_cols]

    def generate_code(self, keep_technical_cols: bool = False) -> str | None:
        """Generate PySpark transformation code based on existing metadata.

        Parameters
        ----------
        keep_technical_cols : bool, optional
            Generate transformations for columns whose names start with underscore, by default False

        Returns
        -------
        str | None
            String containing runnable PySpark code

        Raises
        ------
        ValueError
            Can only generate code based on metadata that is present in memory.
        """
        if self.metadata_df is None:
            msg = "Cannot generate PySpark code from empty metadata."
            raise ValueError(msg)

        mdf = self._keep_dest_cols(self.metadata_df)

        dest_cols = [col for col in mdf["dest_column_name"].to_list() if col] or None
        if not dest_cols:
            log.warning(
                "No destination columns defined in metadata. No code to generate.",
            )
            return None

        if not keep_technical_cols:
            mdf = self._drop_technical_cols(mdf)

        transformations = mdf.sort_values(by=["column_order"]).to_dict(orient="records")
        code_lines = ["df = (df"]
        for t in transformations:
            if (
                isinstance(t["transform_function"], str)
                and len(t["transform_function"]) > 0
            ):
                code_lines.append(
                    f"\t.withColumn(\"{t['dest_column_name']}\", {t['transform_function']})",
                )
            elif (t["source_column_name"] != t["dest_column_name"]) or (
                t["source_column_type"] != t["dest_column_type"]
            ):
                code_lines.append(
                    f"\t.withColumn(\"{t['dest_column_name']}\", F.col(\"{t['source_column_name']}\").cast(\"{t['dest_column_type']}\"))",
                )

        code_lines.append(f"\t.select({dest_cols}))")
        code_str = "\n" + "\n".join(code_lines)

        log.info(code_str)

        return code_str
