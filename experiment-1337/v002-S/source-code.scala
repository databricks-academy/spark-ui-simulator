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
// MAGIC     <td>**2**</td>
// MAGIC     <td>**128 cores**</td>
// MAGIC     <td>**976 GB**</td>
// MAGIC   </tr>
// MAGIC </table>

// COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")

// Disable the Delta IO Cache (reduce side affects)
spark.conf.set("spark.databricks.io.cache.enabled", false)   

// The one, and only one transaction we will be looking for
val trx_id = "1106101026-04-00"

// COMMAND ----------

sc.setJobDescription("Step B: Establish a baseline")

val pathB = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-1tb.delta"

spark
  .read.format("delta").load(pathB)              // Load the non-indexed Delta table
  .filter($"trx_id" === trx_id)                  // Looking for one, and only one transaction
  .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step C: Haystack Query")

val pathC = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-1tb-zo_trx.delta"

spark
  .read.format("delta").load(pathC)              // Load the non-indexed Delta table
  .filter($"z_trx_id" === trx_id)                  // Looking for one, and only one transaction
  .write.format("noop").mode("overwrite").save() // Execute a noop write to test