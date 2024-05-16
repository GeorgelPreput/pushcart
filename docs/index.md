# pushcart

Helps with moving potatoes, bricks and data around.

Pushcart is a metadata-based solution accelerator running on top of Spark. It
also provides a set of ready-made functionalities for data transformations which
might otherwise take a lot of code to put together if only using
`pyspark.sql.functions`.

## Who is this for

- Data engineers writing pure Spark code
- Data engineers working with Databricks Delta Live Tables

## How does the metadata look like?

### Metadata configuration

One-to-one mapping of column transformations, usually used for data processing between
the bronze and silver layers. No aggregations supported. Each metadata row becomes a
`<dataframe>.withColumn(...)` command on the original dataset and at the end of the
transformation steps, columns that do not appear in the `dest_column_name` field are
dropped. Casting is attempted on the fly, where applicable.

### Metadata fields

- `column_order`: Transformations are run in ascending column order.
- `source_column_name`: Original column name expected in the source dataset
- `source_column_type`: Original data type for source column
- `dest_column_name`: Final column name in the destination dataset, after
  transformations have been applied
- `dest_column_type`: Final data type for column to be cast as
- `transform_function`: Pyspark row-level transformation function to be applied
- `default_value`: Value to fill in, should the transformation function or data type
  casting return NULL
- `validation_rule`: Rule to observe when validating data. See
  [Data validation](#data-validation).
- `validation_action`: Action to take when the validation rule is violated. See
  [Data validation](#data-validation).

column_order|source_column_name|source_column_type|dest_column_name|dest_column_type|transform_function|default_value|validation_rule|validation_action
------------|------------------|------------------|----------------|----------------|------------------|-------------|---------------|-----------------
0|id|string|Id|int|||Id IS NOT NULL|DROP
1|first_name|string||||||
2|surname|string||||||
3|||Name|string|"F.concat_ws(' ', F.col('first_name'), F.col('surname'))",F.lit('John Doe')||
4|dob|string|DateOfBirth|date|"F.to_date(F.col('dob'), 'yyyy-MM-dd')"|||
5|record_ts|string|RecordDateTime|timestamp|"F.to_timestamp(F.col('record_ts'), 'yyyy-MM-dd HH:mm:ss')"||RecordDateTime IS NOT NULL|DROP

### Data validation

> **Note**: The data validation functionality has not yet been implemented. The
> description below should reflect the intended implementation at a later date.

Validation will be applied when creating the DLT views / tables.

- **validations**: [_optional_]
  Set of data QA checks to perform on the data when mapping a DLT view on top of it. Validation checks make use of DLT's `@expect` decorators and are written in Spark SQL.

  Components:
  - **validation_rule**: [_required_]
    Rule to observe when validating data. Uses Spark SQL syntax. For example:

    `pk IS NOT NULL` or `some_column_value > 3`

  - **validation_action**: [_required_]
    Action to take when the validation rule is violated. For example:

 Validation action | DLT annotation                           | Result during pipeline run
-------------------|------------------------------------------|-----------------------------------------------------------
 LOG               | `@expect`, `@expect_all`                 | Non-conformant rows are logged, but make it to the target
 DROP              | `@expect_or_drop`, `@expect_all_or_drop` | Non-conformant rows are dropped from the target
 FAIL              | `@expect_or_fail`, `@expect_all_or_fail` | Any non-conformant row will cause the pipeline to fail
