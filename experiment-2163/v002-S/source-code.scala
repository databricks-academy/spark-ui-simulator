// Databricks notebook source
sc.setJobDescription("Step A: Basic initialization")

val trxPath = s"dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.delta" 

// COMMAND ----------

sc.setJobDescription("Step B: Establish a baseline")

// Ensure that the IO Cache is disabled to start with
spark.conf.set("spark.databricks.io.cache.enabled", false)

spark
  .read.format("delta").load(trxPath)            // Load the delta table
  .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step C: Materialize the IO Cache")

// Ensure that the IO Cache is enabled
spark.conf.set("spark.databricks.io.cache.enabled", true)

// Materialize the cache...
spark.read.format("delta").load(trxPath)            // Load the delta table
     .write.format("noop").mode("overwrite").save() // Execute a noop write to materialize

// COMMAND ----------

sc.setJobDescription("Step D: Test #1")

spark.read.format("delta").load(trxPath)            // Load the delta table
     .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step E: Test #2")

spark.read.format("delta").load(trxPath)            // Load the delta table
     .write.format("noop").mode("overwrite").save() // Execute a noop write to test