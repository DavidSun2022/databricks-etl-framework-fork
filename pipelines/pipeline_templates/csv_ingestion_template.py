# Databricks notebook source
# MAGIC %md
# MAGIC ### [User Input Required] Read CSV Source with Autoloader.
# MAGIC
# MAGIC Autoloader Options for CSV: https://docs.databricks.com/en/ingestion/auto-loader/options.html#csv-options
# MAGIC
# MAGIC Common Autoloader Options: https://docs.databricks.com/en/ingestion/auto-loader/options.html#common-auto-loader-options

# COMMAND ----------


from pipelines.shared_utils.autoloader_helper import generated_autoloader_schema_path
from pipelines.shared_utils.autoloader_delta_writer import AutoloaderDeltaWriter

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
datalake_location:str = ""
uc_namespace_location:str = ""

# [User Input Required] Configs
write_mode = ""
trigger_mode = ""

# Instantiate writer classs instance
csv_writer: AutoloaderDeltaWriter = AutoloaderDeltaWriter()

# [User Input Required] Write data.
csv_writer.write_append()

# Other options include
# csv_writer.write_overwrite()
# csv_writer.write_scd_1()
# csv_writer.write_scd_2()
