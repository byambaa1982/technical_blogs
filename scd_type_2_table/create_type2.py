# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

primimary_key = "office_id"

# COMMAND ----------

# Step 0: Define schema and generate mock data
schema = T.StructType([
    T.StructField("office_id", T.StringType(), False),
    T.StructField("address", T.StringType(), True),
    T.StructField("updatedAt", T.TimestampType(), True),
    T.StructField("firm_name", T.StringType(), True)
])

# Create mock data
mock_data = [
    (1, "1232 Main St, Boston, MA", None, "Firm A"),
    (2, "60 Oak Ave, Chicago, IL", None, "Firm B"),
    (3, "79 Pine Rd, Seattle, WA", None, "Firm C"),
    (4, "789 Pine Rd 23, Seattle, WA", None, "Firm D")
]

df = spark.createDataFrame(mock_data, schema).withColumn("updatedAt", F.lit('2025-06-02'))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1: Ingest raw data into CDC enabled Type 2 table
# MAGIC ### Enable CDC on the target table

# COMMAND ----------


spark.sql("""
CREATE TABLE IF NOT EXISTS your_catalog.your_schema.truncated (
    office_id LONG,
    address STRING,
    updatedAt TIMESTAMP,
    firm_name STRING,
    is_current BOOLEAN,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    delta.enableChangeDataFeed = true,
    delta.columnMapping.mode = 'name'
)
""")

# COMMAND ----------



target_table = DeltaTable.forName(spark, "your_catalog.your_schema.truncated")
merge_condition = "source.office_id = target.office_id AND target.is_current = true"
now_ts = F.current_timestamp()

df_scd2 = (
    df.withColumn("is_current", F.lit(True))
      .withColumn("valid_from", F.col("updatedAt"))
      .withColumn("valid_to", F.lit(None).cast("timestamp"))
)
update_set = {
    # Expiring the old record (set as non-current, update valid_to)
    "is_current": F.expr("false"),
    "valid_to": F.expr("source.updatedAt")
}

insert_set = {
    "office_id": "source.office_id",
    "address": "source.address",
    "updatedAt": "source.updatedAt",
    "firm_name": "source.firm_name",
    "is_current": "true",
    "valid_from": "source.updatedAt",
    "valid_to": "cast(null as timestamp)"
}

(
    target_table.alias("target")
    .merge(
        source=df_scd2.alias("source"),
        condition=merge_condition
    )
    .whenMatchedUpdate(
        condition="target.address <> source.address OR target.firm_name <> source.firm_name", # Only update if something changed
        set=update_set
    )
    .whenNotMatchedInsert(values=insert_set)
    .execute()
)

# COMMAND ----------

display(spark.table("your_catalog.your_schema.truncated"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: Create Type 1 transient table

# COMMAND ----------


spark.sql("""
CREATE TABLE IF NOT EXISTS your_catalog.your_schema.transient (
    office_sk LONG,
    office_id STRING,
    address STRING,
    updatedAt TIMESTAMP,
    firm_name STRING
)
USING DELTA
TBLPROPERTIES (delta.columnMapping.mode = 'name')
""")


# COMMAND ----------

last_read_version = 1

# Read CDC updates only since last_read_version
cdc_df = (
    spark.read.format("delta")
    .option("readChangeData", "true")
    .option("startingVersion", last_read_version + 1)
    .table("your_catalog.your_schema.truncated")
    .filter(F.col('is_current') == 'true')
    .filter(F.col("_change_type").isin(["insert", "update"])))


df_scd2 = (
    cdc_df
      .withColumn("valid_from", F.col("updatedAt"))
      .withColumn("valid_to", F.lit(None).cast("timestamp"))
)

display(df_scd2)

# COMMAND ----------

# Get max office_sk from transient table
max_sk = spark.sql("SELECT COALESCE(MAX(office_sk), 0) as max_sk FROM your_catalog.your_schema.transient").collect()[0]["max_sk"]

# Filter for latest changes and assign new SKs
window_spec = Window.orderBy("office_id")
changed_df = (cdc_df
    .filter(F.col("_change_type").isin("insert", "update_postimage"))
    .filter(F.col("is_current") == True)
    .select("office_id", "address", "updatedAt", "firm_name")
    .withColumn("row_num", F.row_number().over(window_spec))
    .withColumn("office_sk", F.col("row_num") + max_sk)
    .drop("row_num")
)

# Merge into Type 1 transient table
transient_delta = DeltaTable.forName(spark, "your_catalog.your_schema.transient")

transient_delta.alias("tgt").merge(
    changed_df.alias("src"),
    "tgt.office_id = src.office_id"
).whenMatchedUpdate(
    set={
        "address": F.col("src.address"),
        "updatedAt": F.col("src.updatedAt"),
        "firm_name": F.col("src.firm_name")
    }
).whenNotMatchedInsert(
    values={
        "office_sk": F.col("src.office_sk"),
        "office_id": F.col("src.office_id"),
        "address": F.col("src.address"),
        "updatedAt": F.col("src.updatedAt"),
        "firm_name": F.col("src.firm_name")
    }
).execute()

# COMMAND ----------

# Display results
display(spark.table("your_catalog.your_schema.truncated"))
display(spark.table("your_catalog.your_schema.transient"))