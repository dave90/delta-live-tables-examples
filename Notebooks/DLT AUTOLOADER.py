# Databricks notebook source
import dlt
from pyspark.sql.functions import *
import random

# COMMAND ----------

@dlt.table(
  comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
)
def clickstream_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema("curr_id INT, n INT, prev_title STRING, curr_title STRING, type STRING")
        .load("dbfs:/mnt/data/wiki-clickstream/")
    )

# COMMAND ----------

@dlt.table
def clickstream_prepared():
  return (
    dlt.read("clickstream_raw")
      .withColumn("click_count", expr("CAST(n AS INT)"))
      .withColumnRenamed("curr_title", "current_page_title")
      .withColumnRenamed("prev_title", "previous_page_title")
      .select("current_page_title", "click_count", "previous_page_title")
  )


# COMMAND ----------

@dlt.table
def top_spark_referrers():
    df = (
        dlt.read(f"clickstream_prepared")
        .withColumnRenamed("previous_page_title", "referrer")
        .sort(desc("click_count"))
        .select("current_page_title","referrer", "click_count")
        .limit(10)
    )
    return df
