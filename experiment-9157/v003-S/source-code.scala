// Databricks notebook source
// MAGIC %md
// MAGIC <table>
// MAGIC   <tr>
// MAGIC     <td></td>
// MAGIC       <td>VM</td>
// MAGIC       <td>Quantity</td>
// MAGIC       <td>Total Cores</td>
// MAGIC       <td>Total RAM</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC       <td>Driver:</td>
// MAGIC       <td>**Standard_DS12_v2**</td>
// MAGIC       <td>**1**</td>
// MAGIC       <td>**4 cores**</td>
// MAGIC       <td>**28.0 GB**</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC       <td>Workers:</td>
// MAGIC       <td>**Standard_DS12_v2**</td>
// MAGIC       <td>**2**</td>
// MAGIC       <td>**8 cores**</td>
// MAGIC       <td>**56 GB**</td>
// MAGIC   </tr>
// MAGIC </table>

// COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")

import org.apache.spark.sql.functions._

// Dataset has about 825 partions, use 832 to maintain factor of cores
spark.conf.set("spark.sql.shuffle.partitions", 832)

// Disable IO cache so as to minimize side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

val ctyPath = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/cities/all.delta"
val trxPath = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb.delta"

// COMMAND ----------

sc.setJobDescription("Step B: Establish a baseline")

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

spark.read.format("delta").load(ctyPath)            // Load the city table
     .filter($"country".substr(0, 1).isin("A","B")) // Countries starting with A or B
     .write.format("noop").mode("overwrite").save() // Test with a noop write

spark.read.format("delta").load(trxPath)            // Load the transactions table
     .write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step C: SMJ Join")

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

val ctyDF = spark
  .read.format("delta").load(ctyPath)                        // Load the city table
  .filter($"country".substr(0, 1).isin("A","B"))             // Countries starting with A or B

val trxDF = spark.read.format("delta").load(trxPath)         // Load the transactions table

trxDF.join(ctyDF, ctyDF("city_id") === trxDF("city_id"))     // Join by city_id
     .write.format("noop").mode("overwrite").save()          // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step D: SHJ Join")

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

spark.conf.set("spark.sql.join.preferSortMergeJoin", false) // Prefer Shuffle Hash Join over Sort Merge Join

val ctyDF = spark
  .read.format("delta").load(ctyPath)                        // Load the city table
  .filter($"country".substr(0, 1).isin("A","B"))             // Countries starting with A or B

val trxDF = spark.read.format("delta").load(trxPath)         // Load the transactions table

trxDF.join(ctyDF, ctyDF("city_id") === trxDF("city_id"))     // Join by city_id
     .write.format("noop").mode("overwrite").save()          // Test with a noop write

// Note Total Time Across All Tasks in the last stage (reducer stage): SMJ's one is slower due to sorting on both sides and hence much more excessive use of memory leading to disk spill.

// COMMAND ----------

sc.setJobDescription("Step E: SHJ Join - AQE")

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", true)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)

spark.conf.set("spark.sql.shuffle.partitions", 912)      // Slightly higher number of shuffle partitions

val ctyDF = spark
  .read.format("delta").load(ctyPath)                    // Load the city table
  .filter($"country".substr(0, 1).isin("A","B"))         // Countries starting with A or B
  .hint("SHUFFLE_HASH")                                  // Enforcing SHUFFLE_HASH using a join hint

val trxDF = spark.read.format("delta").load(trxPath)     // Load the transactions table

trxDF.join(ctyDF, ctyDF("city_id") === trxDF("city_id")) // Join by city_id
     .write.format("noop").mode("overwrite").save()      // Test with a noop write

// Note Total Time Across All Tasks in the last stage (reducer stage): AQE's (with skew handling) one is slightly better .

// COMMAND ----------

sc.setJobDescription("Step F: SHJ Join - OOM")

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

spark.conf.set("spark.sql.shuffle.partitions", 80)           // Lowering number of shuffle partitions to get to bigger partition sizes

val trxDF1 = spark
  .read.format("delta").load(trxPath)                        // Load the transactions table
  .hint("SHUFFLE_HASH")                                      // Enforcing SHUFFLE_HASH using a join hint

val trxDF2 = spark.read.format("delta").load(trxPath)         // Load the transactions table again

trxDF1.join(trxDF2, trxDF1("city_id") === trxDF2("city_id")) // Join a big table with a big table
     .write.format("noop").mode("overwrite").save()          // Test with a noop write
