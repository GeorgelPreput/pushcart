# pushcart

Helps with moving potatoes, bricks and data around.

Pushcart is a metadata-based solution accelerator running on top of Spark. It
also provides a set of ready-made functionalities for data transformations which
might otherwise take a lot of code to put together using only
`pyspark.sql.functions`.

## Who is this for

- Data engineers writing pure Spark code
- Data engineers working with Databricks Delta Live Tables

## How does the metadata look like?

Useful for transforming data from bronze to silver, a metadata specification
looks as such:

column_order|source_column_name|source_column_type|dest_column_name|dest_column_type|transform_function|default_value|validation_rule|validation_action
------------|------------------|------------------|----------------|----------------|------------------|-------------|---------------|-----------------
0|id|string|Id|int|||Id IS NOT NULL|DROP
1|first_name|string||||||
2|surname|string||||||
3|||Name|string|"F.concat_ws(' ', F.col('first_name'), F.col('surname'))",F.lit('John Doe')||
4|dob|string|DateOfBirth|date|"F.to_date(F.col('dob'), 'yyyy-MM-dd')"|||
5|record_ts|string|RecordDateTime|timestamp|"F.to_timestamp(F.col('record_ts'), 'yyyy-MM-dd HH:mm:ss')"||RecordDateTime IS NOT NULL|DROP
