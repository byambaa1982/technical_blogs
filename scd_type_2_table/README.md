

## Introduction

Modern data platforms must efficiently track and process changes in source systems to keep analytics and reporting up to date. **Databricks Change Data Capture (CDC)**, powered by Delta Lake, offers a robust solution for capturing and propagating data changes in real time or batch.

**Key advantages of Databricks CDC:**
- **Incremental Processing:** Only new or changed data is processed, reducing compute costs and improving performance.
- **Built-in Data Lineage:** Delta Lake automatically tracks all changes, making it easy to audit and troubleshoot.
- **Seamless Integration:** CDC works natively with Databricks notebooks, jobs, and SQL, simplifying ETL pipelines.
- **Reliable Consistency:** Delta’s ACID transactions ensure data integrity, even with concurrent updates.
- **Efficient SCD Implementation:** CDC enables straightforward implementation of Slowly Changing Dimensions (SCD), especially SCD Type 2, by exposing change events directly from Delta tables.

In this blog, we’ll demonstrate how to use Databricks CDC and Delta Lake within the Medallion Architecture to build robust, scalable data pipelines:

- The **Bronze layer** will store a full historical record of all changes using an SCD Type 2 table, preserving every version of each record for audit and traceability.
- The **Silver layer** will provide a current-state, fast-access Type 1 table, enhanced with surrogate keys (SKs) for efficient joins and analytics.

### Medallion Architecture Overview
The Medallion Architecture organizes data into progressive layers (Bronze, Silver, Gold) to improve data quality, governance, and performance:
- **Bronze:** Raw, historical, and change-tracked data (SCD2, CDC-enabled) for full lineage and recovery.
- **Silver:** Cleaned, deduplicated, and enriched data (Type 1, with SKs) ready for analytics and BI consumption.
- **Gold:** Aggregated, business-level data marts for reporting and advanced analytics.

**Advantages of the Medallion Architecture:**
- Clear separation of raw, refined, and business-ready data.
- Simplifies data governance and auditing.
- Enables incremental, scalable, and reliable ETL workflows.
- Facilitates rollback, reprocessing, and advanced analytics.

By leveraging Databricks CDC and Delta Lake in this architecture, you ensure your analytics always reflect the latest business reality, with full historical traceability and high performance for downstream consumers.

## Step 1: Understand the Tables

### What is an SCD Type 2 Table?

[image]("/images/type2.png")

- Keeps *all* versions of each record (history).
- Each record has `valid_from` and `valid_to` timestamps, plus an `is_current` flag.
- Typical fields:
  - Natural Key (e.g., `office_id`)
  - Attributes (e.g., address, firm_name, ...)
  - `is_current`, `valid_from`, `valid_to`

**Sample:**
```
| office_id | address                | is_current | valid_from         | valid_to           |
|-----------|------------------------|------------|--------------------|--------------------|
| 1         | 1232 Main St, Boston   | FALSE      | 2022-01-01 10:00   | 2022-06-12 09:00   |
| 1         | 1232 Main St, Cambridge| TRUE       | 2022-06-12 09:00   | NULL               |
```
### What is a Type 1 (Transient) Table?

- Only the latest version of each row (no history).
- Often includes a synthetic surrogate key (`office_sk`)
- This is what your OLAP BI tools will query for up-to-date facts.

**Sample:**
```
| office_sk | office_id | address              | firm_name | updatedAt           |
|-----------|-----------|----------------------|-----------|---------------------|
| 1         | 1         | 1232 Main St, Boston | Firm A    | 2025-06-02 00:00:00 |
```

## Step 2: The Workflow in Databricks (PySpark + Delta)

We'll demonstrate with mock data. Key steps:

1. Generate a mock DataFrame representing new raw changes (from upstream system).
2. Insert these changes into the SCD2 table (using Delta MERGE and CDC).
3. Use CDC feed to update a fast-access (type 1) transient table.

### **1. Prepare Mock Data**

```python
schema = T.StructType([
    T.StructField("office_id", T.StringType(), False),
    T.StructField("address", T.StringType(), True),
    T.StructField("updatedAt", T.TimestampType(), True),
    T.StructField("firm_name", T.StringType(), True)
])
mock_data = [
    (1, "1232 Main St, Boston, MA", None, "Firm A"),
    (2, "60 Oak Ave, Chicago, IL", None, "Firm B"),
    (3, "79 Pine Rd, Seattle, WA", None, "Firm C"),
    (4, "789 Pine Rd 23, Seattle, WA", None, "Firm D")
]
df = spark.createDataFrame(mock_data, schema)\
    .withColumn("updatedAt", F.lit('2025-06-02'))
```
**Expectation:**  
Mock data of 4 "offices" with their addresses, firm names, and an `updatedAt` flag.

```
--------------------------------------------------------------------------
| office_id | address                   | updatedAt           | firm_name |
|-----------|---------------------------|---------------------|-----------|
| 1         | 1232 Main St, Boston, MA  | 2025-06-02 00:00:00 | Firm A    |
| 2         | 60 Oak Ave, Chicago, IL   | 2025-06-02 00:00:00 | Firm B    |
| 3         | 79 Pine Rd, Seattle, WA   | 2025-06-02 00:00:00 | Firm C    |
| 4         | 789 Pine Rd 23, Seattle   | 2025-06-02 00:00:00 | Firm D    |
--------------------------------------------------------------------------
```

### **2. Create SCD Type 2 Table with CDC Enabled**

```python
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
```
- **CDC (Change Data Feed)** is enabled.
- This schema captures SCD2 logic (`is_current`, `valid_from`, `valid_to`).

---

### **3. Insert with SCD2 Merge Logic**

- **If record changed, expire old, insert new.**
- **If new, insert as current.**

```python
target_table = DeltaTable.forName(spark, "your_catalog.your_schema.truncated")
merge_condition = "source.office_id = target.office_id AND target.is_current = true"

df_scd2 = (
    df.withColumn("is_current", F.lit(True))
      .withColumn("valid_from", F.col("updatedAt"))
      .withColumn("valid_to", F.lit(None).cast("timestamp"))
)

update_set = { "is_current": F.expr("false"), "valid_to": F.expr("source.updatedAt") }
insert_set = {
    "office_id": "source.office_id", "address": "source.address", "updatedAt": "source.updatedAt",
    "firm_name": "source.firm_name", "is_current": "true",
    "valid_from": "source.updatedAt", "valid_to": "cast(null as timestamp)"
}

target_table.alias("target").merge(
    source=df_scd2.alias("source"),
    condition=merge_condition
).whenMatchedUpdate(
    condition="target.address <> source.address OR target.firm_name <> source.firm_name",
    set=update_set
).whenNotMatchedInsert(values=insert_set).execute()
```

**Expected SCD2 Table:**
```

| office_id | address                   | updatedAt           | firm_name | is_current | valid_from           | valid_to |
|-----------|---------------------------|---------------------|-----------|------------|----------------------|----------|
| 1         | 1232 Main St, Boston, MA  | 2025-06-02 00:00:00 | Firm A    | TRUE       | 2025-06-02 00:00:00  | NULL     |
| 2         | 60 Oak Ave, Chicago, IL   | 2025-06-02 00:00:00 | Firm B    | TRUE       | 2025-06-02 00:00:00  | NULL     |
| 3         | 79 Pine Rd, Seattle, WA   | 2025-06-02 00:00:00 | Firm C    | TRUE       | 2025-06-02 00:00:00  | NULL     |
| 4         | 789 Pine Rd 23, Seattle, WA| 2025-06-02 00:00:00| Firm D    | TRUE       | 2025-06-02 00:00:00  | NULL     |
```

### **4. Create a Flattened (Type 1) Transient Table**

```python
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
```

---

### **5. Use CDC to Update the Transient Table**

- Only changes since last read (`startingVersion`).
- Only "current" records (`is_current`).
- Assign new surrogate keys (`office_sk`) for new rows.

```python
last_read_version = 1  # Usually tracked externally

cdc_df = (
    spark.read.format("delta")
    .option("readChangeData", "true")
    .option("startingVersion", last_read_version + 1)
    .table("your_catalog.your_schema.truncated")
    .filter(F.col('is_current') == 'true')
    .filter(F.col("_change_type").isin(["insert", "update"]))
)

max_sk = spark.sql("SELECT COALESCE(MAX(office_sk), 0) as max_sk FROM your_catalog.your_schema.transient").collect()[0]["max_sk"]

window_spec = Window.orderBy("office_id")
changed_df = (cdc_df
    .filter(F.col("is_current") == True)
    .select("office_id", "address", "updatedAt", "firm_name")
    .withColumn("row_num", F.row_number().over(window_spec))
    .withColumn("office_sk", F.col("row_num") + max_sk)
    .drop("row_num")
)

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
```

**Expected Transient Table:**
```
| office_sk | office_id | address                   | updatedAt           | firm_name |
|-----------|-----------|---------------------------|---------------------|-----------|
| 1         | 1         | 1232 Main St, Boston, MA  | 2025-06-02 00:00:00 | Firm A    |
| 2         | 2         | 60 Oak Ave, Chicago, IL   | 2025-06-02 00:00:00 | Firm B    |
| 3         | 3         | 79 Pine Rd, Seattle, WA   | 2025-06-02 00:00:00 | Firm C    |
| 4         | 4         | 789 Pine Rd 23, Seattle, WA| 2025-06-02 00:00:00| Firm D    |
```

## Conclusion

By leveraging the Medallion Architecture with Databricks CDC and Delta Lake, you can build scalable, auditable, and high-performance data pipelines:

- The **Bronze layer** (SCD Type 2 table) preserves the full history of all changes, enabling complete auditability and recovery.
- The **Silver layer** (Type 1 table with surrogate keys) provides a clean, up-to-date view of your data, optimized for analytics and BI workloads.
- This layered approach ensures clear separation of raw, refined, and business-ready data, simplifies governance, and supports incremental, reliable ETL workflows.
- All change tracking, history management, and data freshness are handled natively in Delta Lake, making your pipelines easier to maintain and scale.

With this architecture, your analytics always reflect the latest business reality, while maintaining full historical traceability for compliance and advanced analysis.
