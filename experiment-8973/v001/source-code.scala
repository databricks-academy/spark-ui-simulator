// Databricks notebook source
// Partitioned by year, month, day and hour
val sourceDir = "dbfs:/mnt/training/global-sales/transactions/2015.parquet"

val schema = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer, new_at timestamp, year integer, month integer, day integer, hour integer"

val df = spark
  .read
  .schema(schema)
  .parquet(sourceDir)

// COMMAND ----------

df.foreach(x => ())

// COMMAND ----------

// MAGIC %run ./All-Done-Remove-Me
