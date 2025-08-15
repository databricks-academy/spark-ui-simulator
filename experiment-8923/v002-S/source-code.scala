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
// MAGIC     <td>**i3.xlarge**</td>
// MAGIC     <td>**2**</td>
// MAGIC     <td>**8 cores**</td>
// MAGIC     <td>**61 GB**</td>
// MAGIC   </tr>
// MAGIC </table>

// COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")

import org.apache.spark.sql.functions._

// Disable the IO Cache to avoid side effects
spark.conf.set("spark.databricks.io.cache.enabled", "false")

// COMMAND ----------

sc.setJobDescription("Step B: Baseline #1")

// Non-partioned set of transactions, ~41 million records
val path2017 = "dbfs:/mnt/training/global-sales/transactions/2017.parquet"

spark .read.parquet(path2017)                        // Load the 2017 table
      .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step C: Baseline #2")

// Non-partioned set of transactions, ~2.7 billion records
val path2011_2018 = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.parquet"

spark.read.parquet(path2011_2018)                   // Load the 2011-2018 table
     .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step D: Tiny Files")

// Non-partioned set of transactions, ~34 million records
val path2013 = s"dbfs:/mnt/training/global-sales/transactions/2013.parquet"

spark.read.parquet(path2013)                        // Load the 2013 table
     .write.format("noop").mode("overwrite").save() // Execute a noop write to test