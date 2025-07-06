# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.source")  # Create source schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.target")  # Create target schema if it doesn't exist

# COMMAND ----------

scd1 = spark.sql("select * from samples.accuweather.forecast_hourly_metric")
scd1.write.mode("overwrite").saveAsTable("workspace.source.scd1")

# COMMAND ----------

source = spark.read.table('workspace.source.scd1')
source.display()

# COMMAND ----------

from pyspark.sql import functions as F
# Load Data From Source and concatenate all columns into 'ConCatValue'
source = source.withColumn('ConCatValue', F.concat_ws('', *source.columns))
display(source)

# COMMAND ----------

# Add IndCurrent, CreatedDate, and ModifiedDate columns
source = source.withColumn("IndCurrent", F.lit(1)) \
    .withColumn("CreatedDate", F.current_timestamp()) \
    .withColumn("ModifiedDate", F.current_timestamp())
source.display()

# COMMAND ----------

from pyspark.sql.window import Window

window_spec = Window.orderBy(F.monotonically_increasing_id())
source = source.withColumn("storage_id", F.row_number().over(window_spec))

first_cols = ["storage_id"]
other_cols = [col for col in source.columns if col not in first_cols]
source = source.select(first_cols + other_cols)
display(source)

# COMMAND ----------

# Generate SHA-256 hash of concatenated column values and drop 'ConCatValue'
source = source.withColumn("RowHash", F.sha2(F.col("ConCatValue"), 256)).drop('ConCatValue')
display(source)

# COMMAND ----------

#writing to the target schema  
source.write.mode("append").saveAsTable("workspace.target.scd1")
# Display data from the target_table schema

target_df = spark.sql("SELECT * FROM workspace.target.scd1") 
display(target_df)

# COMMAND ----------

SourceTable='workspace.source.scd1'
TargetTable='workspace.target.scd1'

# COMMAND ----------

SourceDf=spark.read.table(SourceTable)  # Read source table into DataFrame
TargetDf=spark.read.table(TargetTable)  # Read target table into DataFrame

# COMMAND ----------

SourceDf.display()

# COMMAND ----------

from pyspark.sql.functions import col

# Filter the DataFrame to show only rows where '("latitude") == "22.36851"'
# Display the filtered DataFrame for inspection
SourceDf.filter(col("latitude") == "22.36851").display()

# The 'city' value for rows with ("latitude") == "22.36851" is 'Pune'

# COMMAND ----------

from pyspark.sql.functions import col, when

# Update the 'city_name' column in SourceDf:
# For rows where "latitude" == "22.36851", set the 'city_name' value to 'Pune'.
# For all other rows, retain the original 'city_name' value.
SourceDf = SourceDf.withColumn(
    "city_name",
    when(col("latitude") == "22.36851", "Pune").otherwise(col("city_name"))
)

# Display rows where "latitude" == "22.36851" to verify the 'city_name' column update.
SourceDf.filter(col("latitude") == "22.36851").display()

# COMMAND ----------

# Create a hash key by concatenating all columns into a single string column 'RowHash'
from pyspark.sql import functions as F

# Concatenate all columns in 'source' DataFrame into 'RowHash'
SourceDf = SourceDf.withColumn('RowHash', F.concat_ws('', *SourceDf.columns))

# COMMAND ----------

# Add three new columns to SourceDf:
# 1. 'IndCurrent': Set to 1 for all rows, indicating the current/active record.
# 2. 'CreatedDate': Set to the current timestamp, representing when the record was created.
# 3. 'ModifiedDate': Set to the current timestamp, representing when the record was last modified.
SourceDf = SourceDf.withColumn("IndCurrent", F.lit(1)) \
    .withColumn("CreatedDate", F.current_timestamp()) \
    .withColumn("ModifiedDate", F.current_timestamp())
SourceDf.display()   

# COMMAND ----------

SourceDf.filter(col("latitude") == "22.36851").display()