// Databricks notebook source
// Reading tiny files
val sourceDir = s"dbfs:/mnt/training/global-sales/transactions/2013.parquet"

val schema = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer, new_at timestamp"

val df = spark
  .read
  .schema(schema)
  .parquet(sourceDir)
  .foreach(x => ())

// COMMAND ----------

// MAGIC %run ./All-Done-Remove-Me
