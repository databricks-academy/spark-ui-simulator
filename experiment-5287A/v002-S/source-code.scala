// Databricks notebook source
// MAGIC %md
// MAGIC <table>
// MAGIC   <tr>
// MAGIC     <td></td>
// MAGIC     <td>VM</td>
// MAGIC     <td>Quantity</td>
// MAGIC     <td>Total Cores</td>
// MAGIC     <td>Total RAM</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>Driver:</td>
// MAGIC     <td>**i3.xlarge**</td>
// MAGIC     <td>**1**</td>
// MAGIC     <td>**4 cores**</td>
// MAGIC     <td>**30.5 GB**</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>Workers:</td>
// MAGIC     <td>**i3.16xlarge**</td>
// MAGIC     <td>**6**</td>
// MAGIC     <td>**384 cores**</td>
// MAGIC     <td>**2928 GB**</td>
// MAGIC   </tr>
// MAGIC </table>

// COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")

import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

// Disable the IO Cache to avoid side effects
spark.conf.set("spark.databricks.io.cache.enabled", "false")

// Free any resources currently in use
spark.catalog.clearCache()

val trxPath = s"dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.delta" 

// COMMAND ----------

sc.setJobDescription("Step B: Establish a baseline")

spark
  .read.format("delta").load(trxPath)            // Load the delta table
  .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step C: Materialize the DF-Cache")

val cachedDF = spark
     .read.format("delta").load(trxPath)                // Load the delta table
     .persist(StorageLevel.DISK_ONLY)                   // Mark the DataFrame as cached

cachedDF.write.format("noop").mode("overwrite").save()  // Execute a noop write to materialize the cache

// COMMAND ----------

sc.setJobDescription("Step D: Test #1")

// Execute a noop write to read from the DF-Cache
cachedDF.write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step E: Test #2")

// Execute a noop write to read from the DF-Cache
cachedDF.write.format("noop").mode("overwrite").save()