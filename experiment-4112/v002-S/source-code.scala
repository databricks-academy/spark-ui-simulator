// Databricks notebook source
sc.setJobDescription("Step A: Basic initialization")

import org.apache.spark.sql.functions._

// Disable IO cache so as to minimize side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

val sourceFile = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.parquet"

// COMMAND ----------

sc.setJobDescription("Step B: Full schema")

val fullSchema = "transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,18), city_id integer"

spark
  .read.schema(fullSchema)                       // Specifying all columns
  .parquet(sourceFile)                           // Load the transactions table
  .write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step C: Partial schema")

val partialSchema = "trx_id string, retailer_id integer, city_id integer"

spark
  .read.schema(partialSchema)                    // Specify only 3 columns
  .parquet(sourceFile)                           // Load the transactions table
  .write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step D: Partial With select() & drop()")

spark
  .read.schema(fullSchema)                                     // Specifying all columns
  .parquet(sourceFile)                                         // Load the transactions table
  .select("trx_id", "retailer_id", "city_id", "transacted_at") // Select 4 columns
  .drop("transacted_at")                                       // Drop the 4th column
  .write.format("noop").mode("overwrite").save()               // Test with a noop write