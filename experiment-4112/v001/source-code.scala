// Databricks notebook source
import org.apache.spark.sql.functions._

spark.conf.set("spark.sql.files.maxPartitionBytes", "1280m") // 1.25gb, full read of part files
spark.conf.set("spark.databricks.io.cache.enabled", "false") // Disable the Delta IO Cache

val sourceFile = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.parquet"

// COMMAND ----------

// Full read of all columns
val schemaA = "transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,18), city_id integer"

spark.read.schema(schemaA).parquet(sourceFile).foreach(_=> ())

// COMMAND ----------

// Schema reduces columns
val schemaB = "trx_id string, retailer_id integer, city_id integer"

spark.read.schema(schemaB).parquet(sourceFile).foreach(_=> ())

// COMMAND ----------

// select() & drop() reduces columns
val schemaC = "transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,18), city_id integer"

spark.read.schema(schemaC).parquet(sourceFile)
     .select("trx_id", "retailer_id", "city_id", "transacted_at")
     .drop("transacted_at")
     .foreach(_=> ())

// COMMAND ----------

// MAGIC %run ./All-Done-Remove-Me
