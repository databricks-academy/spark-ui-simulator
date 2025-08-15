// Databricks notebook source
import org.apache.spark.sql.functions._

// Default partion size
spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")

// Disable the Delta cache to avoid side effects
spark.conf.set("spark.databricks.io.cache.enabled", "false")

// COMMAND ----------

// Establish a Baseline
val trxDF = spark
  .read.format("delta")
  .load("dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.delta")

trxDF.foreach(x => ())

// COMMAND ----------

// Materialize the cache
val trxDF = spark
  .read.format("delta")
  .load("dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.delta")
  .persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)

trxDF.foreach(x => ())

// COMMAND ----------

// Post-Cache
trxDF.foreach(x => ())
