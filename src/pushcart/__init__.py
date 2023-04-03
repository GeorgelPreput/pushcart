from functools import lru_cache

from pyspark.sql import SparkSession

try:
    spark
except NameError:
    spark = None

if not spark:
    spark = (
        SparkSession.builder.config("spark.driver.host", "localhost")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


sc = spark.sparkContext
sql = spark.sql

try:
    from pyspark.dbutils import DBUtils

    dbutils, __dbutils = DBUtils(spark), DBUtils(spark)

    @lru_cache(maxsize=50)
    def __get_cached_secret(scope: str, key: str) -> str:
        return __dbutils.secrets.get(scope, key)

    dbutils.secrets.get = __get_cached_secret
except ImportError:
    from pushcart.dbutils import DBUtils

    dbutils = DBUtils()


__all__ = ["configuration", "setup", "stages"]
