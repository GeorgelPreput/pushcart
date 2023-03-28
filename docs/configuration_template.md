# Configuration templates

## File hierarchy

Configuration files are the building blocks of DLT pipeline specifications. Any number of such files that reside within a folder will be parsed together into a dictionary which describe the totality of objects within a DLT pipeline.

The DLT pipeline is named after the folder name holding its configuration files, with the pattern `<folder_name>__dlt`. Should one or more of the sources in this pipeline rely on building a preliminary checkpoint, a separate traditional Databricks job called `<folder_name>__checkpoint` will also be created during the release process.

### Sample folder structure

- `/pipelines`
  - `../main_schema`
    - `../veggie_pipeline`
      - `bronze__potato_table.[yaml|json|toml]`
      - `silver__potato_table.[yaml|json|toml]`
      - `silver__potato_table.csv`
      - `bronze__carrot_table.[yaml|json|toml]`
      - `silver__carrot_table.[yaml|json|toml]`
      - `silver__carrot_table.csv`
      - `gold__stew_table.[yaml|json|toml]`
      - `...`
    - `../meat_pipeline`
      - `bronze__steak_table.[yaml|json|toml]`
      - ...
  - `../desserts_schema`
    - `../dairy_pipeline`
      - `bronze__ice_cream_table.[yaml|json|toml]`
      - `...`

Just as one or more configuration file can be part of one and only one pipeline, so will a pipeline write to one schema.  
_* This is a Databricks limitation, and may be subject to change._

What follows is the specification of the above-mentioned configuration files, each holding at least one of the three stages for data movement, `sources`, `transformations` and `destinations` as arrays of configurable objects. Configuration files can be defined in YAML, JSON or TOML, and sometimes may be accompanied by CSV metadata for the transformation stage.

- **sources**: [_optional_]  
  Array of data source definitions

- **transformations**: [_optional_]  
  Array of transformations on datasets already defined in the pipeline

- **destinations**: [_optional_]  
  Array of tables to write data to

### High-level sample configuration

```yaml
sources:
- ...

transformations:
- ...

destinations:
- ...
```

## Sources

The `sources` field is composed of an array of one or more source objects, each defined using the following fields:

- **from**: [_required_]  
  Location of data source, could be a table name, a path to a Delta Table folder, a URL, etc.
  
  Examples:
  - `metastore.schema.table_name`
  - `ext://path/to/delta/table/or/autoloader/files`
  - `https://sample.website.com/path/to/rest/api`

- **type**: [_required_]  
  Type of data source. By default, the library supports Spark tables, Delta Table paths, and Autoloader folders.

  Examples: `table`, `delta`, `autoloader`

- **into**: [_required_]  
  Name of DLT view to map over external data source. Must be unique within a pipeline. Higly recommended to stick to a naming convention consistent throughout multiple layers.

  Examples: `vw__raw__schema_table_name`, `vw__bronze__table_name_transformed`

- **params**: [_optional_]  
  Set of parameters to send to the source reader. Exact parameter specifications to vary based on reader implementation. Parameters are sent as a dictionary to the reader object.

  Examples:
  - `format: json`
  - `limit: 10000`
  - `columns: [ pk, and, some, other, columns ]`

- **validations**: [_optional_]  
  See [Data validation](#data-validation) for details.

### Sample sources

- **YAML**

  ```yaml
  sources:
  - from: metastore.schema.table_name
    type: table
    into: vw__raw__schema_table_name

  - from: ext://path/to/delta/table
    type: delta
    into: vw__raw__delta_table_name
  ```

- **JSON**

  ```json
  "sources": [
      {
          "from": "ext://path/to/autoloader/folder",
          "type": "autoloader",
          "into": "vw__raw__data_source_name",
          "validations": [
              {
                  "validation_rule": "pk IS NOT NULL",
                  "validation_action": "DROP"
              }
          ]
      }
  ]
  ```

## Transformations

Also composed of an array of incremental transformations, each transformation object applies onto an existing DLT view or table, as defined within the other sections of the pipeline, and produces its result in a DLT view as well. The order of transformations is defined in a DAG (directed, acyclical graph) composed of nodes (`from` and `into` views) and edges (the upstream/downstream relationships between them).

- **from**: [_required_]  
  View or table name to apply transformation to. Should already have been defined as target `into` field in a previous item within `sources`, `transformations` or `destinations`.

  Examples: `vw__bronze__some_table_name`, `vw__silver__table_name_applied_transformation`

- **into**: [_required_]
  View to hold the result of the transformation step. Must be unique within a pipeline.

  Examples: `vw__bronze__table_name_filtered`, `vw__gold__table_standard_deviation_agg`

- **config**: [_optional_]  
  **Either `config` or `sql_query` _IS_ required!** See [Metadata configuration](#metadata-configuration) for details.

- **sql_query**: [_optional_]  
  **Either `config` or `sql_query` _IS_ required!** Represents the Spark SQL query to apply to a full dataset as a transformation step. It is highly recommended to split monolithical SQL queries into small, easy to follow steps and map each to a transformation.

  Examples:
  - `SELECT month, count(1) FROM vw__silver__time_series GROUP BY month`
  - `SELECT a.one, b.two FROM vw__gold__table_a a JOIN vw__gold__table_b b ON a.pk = b.pk`
  - `SELECT * FROM vw__silver__full_table WHERE __END_AT IS NULL`

- **validations**: [_optional_]  
  See [Data validation](#data-validation) for details.

### Sample transformations

- **YAML**

  ```yaml
  transformations:
  - from: vw__silver__item_cost
    into: vw__silver__item_cost_monthly_avg
    sql_query: |
      SELECT month, item_id, avg(price) AS average_price
      FROM vw__silver__item_cost
      GROUP BY item_id, month 

  - from: vw__silver__item_cost_monthly_avg
    into: vw__silver__item_cost_monthly_avg_summer
    sql_query: |
      SELECT * FROM vw__bronze__item_cost_monthly_avg
      WHERE month IN ('june', 'july', 'august')
    validations:
    - validation_rule: item_id IS NOT NULL
      validation_action: DROP
  ```

- **JSON**

  ```json
  "transformations": [
      {
          "from": "vw__bronze__it_events",
          "into": "vw__silver__it_events",
          "config": "./it_events.csv"
      }
  ]
  ```

## Destinations

Array of target DLT tables to write into. These tables must not already be managed by another pipeline.

- **from**: [_required_]  
  DLT view to use as data source. The data presented by this view will be written to disk without any further transformations

  Examples: `vw__raw__some_table_name` or `vw__bronze__some_other_table_transformed`

- **into**: [_required_]  
  Name of Delta table to write to. Schema name shall be omitted at this level, as it is set per pipeline. Within a pipeline, all the tables and views declared as targets have to be unique.

  Examples: `bronze__some_table_name`, `silver__some_other_table`

- **path**: [_optional_]  
  If set, the created table will be external, supported by files at this location. If not set, created table will be managed by Databricks internally. **Databricks _MUST_ have configured access to the external storage location prior to the pipeline running.**

  Examples:  
  - `s3://path/to/bucket/folder/to/store/delta/table`
  - `abfss://container@storageaccount.dfs.core.windows.net/path/to/folder`
  - `gs://folders/where/data/is/stored`

- **mode**: [_required_]  
  Write mode for table. Accepts either `append` or `upsert`. Upserting will result in an SCD type 2 being created.

### Sample destinations

- **YAML**

  ```yaml
  destinations:
  - from: vw__bronze__sample_table_transformed
    into: vw__silver__sample_table
    path: ext://schema_folder/silver/sample_table
    mode: upsert
  ```

- **JSON**

  ```json
  "destinations": [
    {
        "from": "vw__raw__sample_table",
        "into": "vw__bronze__sample_table",
        "path": "ext://schema_folder/bronze/sample_table",
        "mode": "append"
    }
  ]
  ```

## Data validation

Validation sections can appear in any stage of a data pipeline (`sources`, `transformations`, `destinations`), and are applied when creating the DLT views / tables.

- **validations**: [_optional_]  
  Set of data QA checks to perform on the data when mapping a DLT view on top of it. Validation checks make use of DLT's `@expect` decorators and are written in Spark SQL.

  Components:
  - **validation_rule**: [_required_]  
    Rule to observe when validating data. Uses Spark SQL syntax

    Examples: `pk IS NOT NULL` or `some_column_value > 3`

  - **validation_action**: [_required_]  
    Action to take when the validation rule is violated.

    Examples:  
    | Validation action | DLT annotation                           | Result during pipeline run                                |
    |-------------------|------------------------------------------|-----------------------------------------------------------|
    | LOG               | `@expect`, `@expect_all`                 | Non-conformant rows are logged, but make it to the target |
    | DROP              | `@expect_or_drop`, `@expect_all_or_drop` | Non-conformant rows are dropped from the target           |
    | FAIL              | `@expect_or_fail`, `@expect_all_or_fail` | Any non-conformant row will cause the pipeline to fail    |

## Metadata configuration

One-to-one mapping of column transformations, usually used for data processing between the bronze and silver layers. No aggregations supported. Each metadata row becomes a `<dataframe>.withColumn(...)` command on the original dataset and at the end of the transformation steps, columns that do not appear in the `dest_column_name` field are dropped. Casting is attempted on the fly, where applicable.

### Metadata fields

- `column_order`: Transformations are run in ascending column order.
- `source_column_name`: Original column name expected in the source dataset
- `source_column_type`: Original data type for source column
- `dest_column_name`: Final column name in the destination dataset, after transformations have been applied
- `dest_column_type`: Final data type for column to be cast as
- `transform_function`: Pyspark row-level transformation function to be applied
- `default_value`: Value to fill in, should the transformation function or data type casting return NULL
- `validation_rule`: Rule to observe when validating data. See [Data validation](#data-validation).
- `validation_action`: Action to take when the validation rule is violated. See [Data validation](#data-validation).

### Sample transformation metadata

| column_order | source_column_name | source_column_type | dest_column_name | dest_column_type | transform_function                                        | default_value     | validation_rule            | validation_action |
|--------------|--------------------|--------------------|------------------|------------------|-----------------------------------------------------------|-------------------|----------------------------|-------------------|
| 0            | id                 | string             | Id               | int              |                                                           |                   | Id IS NOT NULL             | DROP              |
| 1            | first_name         | string             |                  |                  |                                                           |                   |                            |                   |
| 2            | surname            | string             |                  |                  |                                                           |                   |                            |                   |
| 3            |                    |                    | Name             | string           | F.concat_ws(" ", F.col("first_name"), F.col("surname"))   | F.lit("John Doe") |                            |                   |
| 4            | dob                | string             | DateOfBirth      | date             | F.to_date(F.col("dob"), 'yyyy-MM-dd')                     |                   |                            |                   |
| 5            | record_ts          | string             | RecordDateTime   | timestamp        | F.to_timestamp(F.col("record_ts"), "yyyy-MM-dd HH:mm:ss") |                   | RecordDateTime IS NOT NULL | DROP              |
