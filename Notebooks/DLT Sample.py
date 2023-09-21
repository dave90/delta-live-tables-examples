# Databricks notebook source
import dlt
from pyspark.sql.functions import *
import random

# COMMAND ----------

json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"

# COMMAND ----------

TABLES_PREPROD = 3

# COMMAND ----------

@dlt.table(
  comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
)
def clickstream_raw():
  return (spark.read.format("json").load(json_path))

# COMMAND ----------

def clickstream_prepared_rand(r: int):
  return (
    dlt.read("clickstream_raw")
      .withColumn("click_count", expr("CAST(n AS INT)"))
      .withColumnRenamed("curr_title", "current_page_title")
      .withColumnRenamed("prev_title", "previous_page_title")
      .filter(col("click_count") > r)
      .select("current_page_title", "click_count", "previous_page_title")
  )

for i in range(TABLES_PREPROD):
    @dlt.table(name=f"clickstream_prepared_{i}")
    def clickstream_prepared():
        r = random.randint(1,100)
        return clickstream_prepared_rand(r)

# COMMAND ----------

@dlt.table
def top_spark_referrers():
  df_join = None
  for i in range(TABLES_PREPROD):
    df = (
        dlt.read(f"clickstream_prepared_{i}")
        .filter(expr("current_page_title == 'Apache_Spark'"))
        .withColumnRenamed("previous_page_title", "referrer")
        .sort(desc("click_count"))
        .select("referrer", "click_count")
        .limit(10)
    )
    if df_join is None:
        df_join = df
    else:
        df_join = df_join.union(df)
  return df_join

# COMMAND ----------


