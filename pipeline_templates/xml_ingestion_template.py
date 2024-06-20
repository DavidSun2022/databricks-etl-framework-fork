# Databricks notebook source
# MAGIC %md
# MAGIC ### [User Input Required] Read CSV Source with Autoloader.
# MAGIC
# MAGIC Autoloader Options for CSV: https://docs.databricks.com/en/ingestion/auto-loader/options.html#csv-options
# MAGIC
# MAGIC Common Autoloader Options: https://docs.databricks.com/en/ingestion/auto-loader/options.html#common-auto-loader-options

# COMMAND ----------

from ingestion_framework.pipelines.shared_utils.streaming_delta_writer import AutoloaderWriter, WriteMode, RefreshMode, TriggerMode
from pipelines.shared_utils.autoloader_helper import generated_autoloader_schema_path

# [User Input Required] Set the ingest location.
ingest_location = "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow"

# Auto-Generate Schema Location Based on Ingest Location
autoloader_schema_location = generated_autoloader_schema_path(ingest_location)
print("Autoloader Schema Location: " +autoloader_schema_location)

# [User Input Required] Configure Autoloader settings
df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.schemaLocation", autoloader_schema_location) # Required
    # [Input Required] Common Autoloader Settings
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.maxFilesPerTrigger", "1000")
    # [Input Required] Error Handling Settings
    .option("ignoreCorruptFiles", "false")
    # [Input Required] CSV Autoloader Settings
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema","true")
    .load(ingest_location)
)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### [User Input Required] Optional Transformations

# COMMAND ----------


df.createOrReplaceTempView("tmp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM
# MAGIC tmp_view

# COMMAND ----------

# MAGIC %md
# MAGIC ### [User Input Required] Write Data to the Lake

# COMMAND ----------

# [User Input Required] Result DataFrame
output_df = _sqldf

# [User Input Required] Target Location
catalog = ""
schema = ""
table = ""

# [User Input Required] Configs
write_mode = WriteMode.APPEND
refresh_mode = RefreshMode.INCREMENTAL
trigger_mode = TriggerMode.TRIGGERED

# COMMAND ----------

# Write data into the Data Lake and/or UC
csv_writer: AutoloaderWriter = AutoloaderWriter(output_df)
csv_writer.write_uc_external_table(
    uc_catalog=catalog,
    uc_schema=schema,
    uc_table=table,
    write_mode=write_mode,
    refresh_mode=refresh_mode,
    trigger_mode=trigger_mode,
)
