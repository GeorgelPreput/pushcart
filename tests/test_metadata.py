"""Tests for PushCart metadata."""

import pytest
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pushcart.metadata import Metadata


@pytest.fixture
def spark():
    """Make spark available for the test suite.

    Yields
    ------
    SparkSession
        Existing or new SparkSession object
    """
    yield SparkSession.builder.getOrCreate()


def test_metadata_transformations(spark):  # pylint: disable=redefined-outer-name
    """Test applying the metadata transformations to a sample dataset."""

    input_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("escape", '"')
        .load("tests/data/sample_data.csv")
    )

    md = Metadata(input_df, infer_fraction=1.0)
    md.get_metadata()

    transformed_data = md.transform()

    assert transformed_data.schema == T.StructType(
        [
            T.StructField("id", T.StringType()),
            T.StructField("ts", T.TimestampType()),
            T.StructField(
                "payload",
                T.StructType(
                    [
                        T.StructField("current_page", T.LongType()),
                        T.StructField(
                            "data",
                            T.ArrayType(
                                T.StructType(
                                    [
                                        T.StructField("fact", T.StringType()),
                                        T.StructField("length", T.LongType()),
                                    ],
                                ),
                            ),
                        ),
                        T.StructField("first_page_url", T.StringType()),
                        T.StructField("from", T.LongType()),
                        T.StructField("last_page", T.LongType()),
                        T.StructField("last_page_url", T.StringType()),
                        T.StructField(
                            "links",
                            T.ArrayType(
                                T.StructType(
                                    [
                                        T.StructField("active", T.BooleanType()),
                                        T.StructField("label", T.StringType()),
                                        T.StructField("url", T.StringType()),
                                    ],
                                ),
                            ),
                        ),
                        T.StructField("next_page_url", T.StringType()),
                        T.StructField("path", T.StringType()),
                        T.StructField("per_page", T.LongType()),
                        T.StructField("prev_page_url", T.StringType()),
                        T.StructField("to", T.LongType()),
                        T.StructField("total", T.LongType()),
                    ],
                ),
            ),
        ],
    )
    assert transformed_data.count() == input_df.count()


def test_metadata_code_generation(spark):  # pylint: disable=redefined-outer-name
    """Test metadata code generation from a sample dataset."""

    input_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("escape", '"')
        .load("tests/data/sample_data.csv")
    )

    md = Metadata(input_df, infer_fraction=1.0)
    md.get_metadata(show=False)

    code = md.generate_code()

    assert (
        code
        == """
df = (df
	.withColumn("id", F.col("id"))
	.withColumn("ts", F.to_timestamp(F.col("ts"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
	.withColumn("payload", F.from_json(F.col("payload"), schema="struct<current_page:bigint,data:array<struct<fact:string,length:bigint>>,first_page_url:string,from:bigint,last_page:bigint,last_page_url:string,links:array<struct<active:boolean,label:string,url:string>>,next_page_url:string,path:string,per_page:bigint,prev_page_url:string,to:bigint,total:bigint>"))
	.select(['id', 'ts', 'payload']))"""
    )


def test_metadata_nested_transformations(spark):  # pylint: disable=redefined-outer-name
    """Test applying the metadata transformations to a sample nested dataset."""

    flattened_row_count = 1350

    input_nested_data = spark.read.format("parquet").load(
        "tests/data/sample_nested_data.parquet"
    )

    nested_md = Metadata(input_nested_data, infer_fraction=1.0)
    nested_md.get_metadata(show=False)

    transformed_flattened_data = nested_md.transform()

    assert transformed_flattened_data.schema == T.StructType(
        [
            T.StructField("id", T.StringType()),
            T.StructField("ts", T.TimestampType()),
            T.StructField("payload_current_page", T.LongType()),
            T.StructField(
                "payload_data",
                T.StructType(
                    [
                        T.StructField("fact", T.StringType()),
                        T.StructField("length", T.LongType()),
                    ]
                ),
            ),
            T.StructField("payload_data_fact", T.StringType()),
            T.StructField("payload_data_length", T.LongType()),
            T.StructField("payload_first_page_url", T.StringType()),
            T.StructField("payload_from", T.LongType()),
            T.StructField("payload_last_page", T.LongType()),
            T.StructField("payload_last_page_url", T.StringType()),
            T.StructField(
                "payload_links",
                T.StructType(
                    [
                        T.StructField("active", T.BooleanType()),
                        T.StructField("label", T.StringType()),
                        T.StructField("url", T.StringType()),
                    ]
                ),
            ),
            T.StructField("payload_links_active", T.BooleanType()),
            T.StructField("payload_links_label", T.StringType()),
            T.StructField("payload_links_url", T.StringType()),
            T.StructField("payload_next_page_url", T.StringType()),
            T.StructField("payload_path", T.StringType()),
            T.StructField("payload_per_page", T.LongType()),
            T.StructField("payload_prev_page_url", T.StringType()),
            T.StructField("payload_to", T.LongType()),
            T.StructField("payload_total", T.LongType()),
        ]
    )
    assert transformed_flattened_data.count() == flattened_row_count


def test_metadata_nested_code_generation(spark):  # pylint: disable=redefined-outer-name
    """Test metadata code generation from a sample nested dataset."""

    input_nested_data = spark.read.format("parquet").load(
        "tests/data/sample_nested_data.parquet"
    )

    nested_md = Metadata(input_nested_data, infer_fraction=1.0)
    nested_md.get_metadata(show=False)

    code = nested_md.generate_code()

    assert (
        code
        == """
df = (df
	.withColumn("id", F.col("id"))
	.withColumn("ts", F.col("ts"))
	.withColumn("payload_current_page", F.col("payload.current_page"))
	.withColumn("payload_data", F.explode("payload.data"))
	.withColumn("payload_data_fact", F.col("payload_data.fact"))
	.withColumn("payload_data_length", F.col("payload_data.length"))
	.withColumn("payload_first_page_url", F.col("payload.first_page_url"))
	.withColumn("payload_from", F.col("payload.from"))
	.withColumn("payload_last_page", F.col("payload.last_page"))
	.withColumn("payload_last_page_url", F.col("payload.last_page_url"))
	.withColumn("payload_links", F.explode("payload.links"))
	.withColumn("payload_links_active", F.col("payload_links.active"))
	.withColumn("payload_links_label", F.col("payload_links.label"))
	.withColumn("payload_links_url", F.col("payload_links.url"))
	.withColumn("payload_next_page_url", F.col("payload.next_page_url"))
	.withColumn("payload_path", F.col("payload.path"))
	.withColumn("payload_per_page", F.col("payload.per_page"))
	.withColumn("payload_prev_page_url", F.col("payload.prev_page_url"))
	.withColumn("payload_to", F.col("payload.to"))
	.withColumn("payload_total", F.col("payload.total"))
	.select(['id', 'ts', 'payload_current_page', 'payload_data', 'payload_data_fact', 'payload_data_length', 'payload_first_page_url', 'payload_from', 'payload_last_page', 'payload_last_page_url', 'payload_links', 'payload_links_active', 'payload_links_label', 'payload_links_url', 'payload_next_page_url', 'payload_path', 'payload_per_page', 'payload_prev_page_url', 'payload_to', 'payload_total']))"""  # pylint: disable=line-too-long
    )
