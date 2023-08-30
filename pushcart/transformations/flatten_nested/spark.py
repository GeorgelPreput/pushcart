import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql import DataFrame


def flatten_struct(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + "." + field.name if prefix else field.name
        if isinstance(field.dataType, T.StructType):
            fields += flatten_struct(field.dataType, prefix=name)
        else:
            fields.append(name)

    return fields


def get_array_columns(schema):
    return [
        field.name for field in schema.fields if isinstance(field.dataType, T.ArrayType)
    ]


def flatten(df: DataFrame) -> DataFrame:
    array_columns = get_array_columns(df.schema)

    df = df.select(
        *[
            F.col(col_name).alias(col_name.replace(".", "__"))
            for col_name in flatten_struct(df.schema)
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
        [
            isinstance(field.dataType, T.StructType)
            or isinstance(field.dataType, T.ArrayType)
            for field in df.schema.fields
        ]
    ):
        return flatten(df)

    return df
