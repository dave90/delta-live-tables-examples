# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.table()
def rating_raw():
    
    return ( 
        spark.readStream 
        .format("kafka")
        .option("kafka.bootstrap.servers", spark.conf.get("kafka.bootstrap.servers"))
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.session.timeout.ms", "45000") 
        .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(dbutils.secrets.get("function-scope","KAFKA-ID"), dbutils.secrets.get("function-scope","KAFKA-KEY")))
        .option("subscribe", spark.conf.get("kafka.subscribe")) 
        .option("startingOffsets", "latest") 
        .load()
    )

# COMMAND ----------

@dlt.table()
def rating_preproc():
    schema = "overall DOUBLE, verified BOOLEAN, reviewText STRING, reviewerName STRING"

    return ( 
        dlt.read("rating_raw") 
        .select(from_json(col("value").cast("string"), schema ).alias("data"))
        .select("data.overall","data.verified","data.reviewText","data.reviewerName")
    )

# COMMAND ----------

@dlt.table()
def reviewer_score():

    return ( 
        dlt.read("rating_preproc") 
        .select("overall","reviewerName")
        .filter(col("overall").isNotNull() & col("reviewerName").isNotNull())
        .groupby("reviewerName").agg(
            count("reviewerName").alias("count_review"),
            avg("overall").alias("score")
        )
        .orderBy(col("count_review").desc())
    )

# COMMAND ----------

@dlt.table()
def top_review():
    return ( 
        dlt.read("rating_preproc") 
        .select("overall","reviewerName","reviewText")
        .filter(col("overall").isNotNull() & col("reviewerName").isNotNull() & col("reviewText").isNotNull())
        .orderBy(col("overall").desc())
    )

# COMMAND ----------

@dlt.table()
def worst_review():
    return ( 
        dlt.read("rating_preproc") 
        .select("overall","reviewerName","reviewText")
        .filter(col("overall").isNotNull() & col("reviewerName").isNotNull() & col("reviewText").isNotNull())
        .orderBy(col("overall").asc())
    )
