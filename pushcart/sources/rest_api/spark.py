"""Get a PySpark DataFrame from a REST API request."""

import pyspark.sql.functions as F  # noqa: N812
import validators
from pyspark.sql import DataFrame, Row, SparkSession

from pushcart.sources.rest_api.request import batch_request


def request_dataframe(  # noqa: PLR0913
    url: str,
    headers: dict = None,
    auth: dict = None,
    params: dict = None,
    data: str = None,
    json_data: dict = None,
    method: str = "GET",
    options: dict = None,
    schema: str = "string",
) -> DataFrame:
    """Get a PySpark DataFrame by running a REST API request through a UDF.

    Parameters
    ----------
    url : str
        URL to call
    headers : dict
        REST API headers to pass
    auth : dict
        Dictionary providing authentication details. Sample values below:
        No authentication: None or { "type": "NO_AUTH" }
        Basic authentication: { "type": "BASIC", "client-id": "...", "client-secret": "..." }
        OAuth2: { "type": "OAUTH2", "token-url": "...", "client-id": "...", "client-secret": "..." }
    params : dict
        Dictionary of parameters to send with the API request
    data : str
        Text data to be sent in the body of the request. Excludes the use of the
        json_data parameter
    json_data : dict
        Dictionary to be converted to a JSON string and sent in the body of the
        request. Excludes the use of the data parameter.
    method : str
        "GET" or "POST", by default "GET"
    options : dict
        Dictionary of options to send to the httpx.Client request function. For more
        info, see: https://www.python-httpx.org/api/
    schema : str
        Spark schema for the expected return data, by default "string". Format of the
        schema string follows DataFrame.schema.simpleString() output.

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame containing the API request and response.
    """
    validators.url(url)

    headers = headers or {}
    auth = auth or {}
    params = params or {}
    data = data or ""
    json_data = json_data or {}
    options = options or {}

    spark = SparkSession.builder.getOrCreate()
    udf_batch_request = F.udf(batch_request, returnType=schema)

    rest_api_row = Row(
        "url",
        "headers",
        "auth",
        "params",
        "data",
        "json_data",
        "method",
        "options",
    )
    request_df = spark.createDataFrame(
        [
            rest_api_row(
                url,
                headers,
                auth,
                params,
                data,
                json_data,
                method,
                options,
            ),
        ],
        schema="struct<url: string, headers:map<string, string>, auth: map<string, string>, params: map<string, string>, data: string, json_data: map<string, string>, method: string, options: map<string, string>>",
    )

    return request_df.withColumn(
        "result",
        udf_batch_request(
            F.col("url"),
            F.col("headers"),
            F.col("auth"),
            F.col("params"),
            F.col("data"),
            F.col("json_data"),
            F.col("method"),
            F.col("options"),
        ),
    )
