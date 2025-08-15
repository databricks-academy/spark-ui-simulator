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
// MAGIC       <td>**i3.xlarge**</td>
// MAGIC       <td>**1**</td>
// MAGIC       <td>**4 cores**</td>
// MAGIC       <td>**30.5 GB**</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC       <td>Workers:</td>
// MAGIC       <td>**i3.xlarge**</td>
// MAGIC       <td>**4**</td>
// MAGIC       <td>**16 cores**</td>
// MAGIC       <td>**122 GB**</td>
// MAGIC   </tr>
// MAGIC </table>

// COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")

import org.apache.spark.sql.functions._

// Dataset has about 825 partions, we use auto to leverage AQE
spark.conf.set("spark.sql.shuffle.partitions", 1000)

// Disable IO cache so as to minimize side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

val ctyPath = "dbfs:/mnt/training/global-sales/cities/all.delta"
val trxPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-zo_city.delta"

// COMMAND ----------

sc.setJobDescription("Step B: Establish a baseline")

spark.read.format("delta").load(ctyPath)            // Load the city table
     .write.format("noop").mode("overwrite").save() // Test with a noop write

spark.read.format("delta").load(trxPath)            // Load the transactions table
     .write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step C-1: SMJ Join w/o AQE")

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

val ctyDF = spark
  .read.format("delta").load(ctyPath)                        // Load the city table

val trxDF = spark.read.format("delta").load(trxPath)         // Load the transactions table

trxDF.join(ctyDF, ctyDF("city_id") === trxDF("z_city_id"))     // Join by city_id
     .write.format("noop").mode("overwrite").save()          // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step C-2: SMJ Join with AQE")

// Enable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", true)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)

val ctyDF = spark
  .read.format("delta").load(ctyPath)                        // Load the city table

val trxDF = spark.read.format("delta").load(trxPath)         // Load the transactions table

trxDF.join(ctyDF, ctyDF("city_id") === trxDF("z_city_id"))     // Join by city_id
     .write.format("noop").mode("overwrite").save()          // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step D-1: SHJ Join w/o AQE")

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)


val ctyDF = spark
  .read.format("delta").load(ctyPath)                        // Load the city table
  .hint("SHUFFLE_HASH")

val trxDF = spark.read.format("delta").load(trxPath)         // Load the transactions table

trxDF.join(ctyDF, ctyDF("city_id") === trxDF("z_city_id"))     // Join by city_id
     .write.format("noop").mode("overwrite").save()          // Test with a noop write

// Note Total Time Across All Tasks in the last stage (reducer stage): SMJ's one is slower due to sorting on both sides and hence much more excessive use of memory leading to disk spill.

// COMMAND ----------

sc.setJobDescription("Step D-2: SHJ Join with AQE")

// Enable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", true)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)


val ctyDF = spark
  .read.format("delta").load(ctyPath)                    // Load the city table
  .hint("SHUFFLE_HASH")                                  // Enforcing SHUFFLE_HASH using a join hint

val trxDF = spark.read.format("delta").load(trxPath)     // Load the transactions table

trxDF.join(ctyDF, ctyDF("city_id") === trxDF("z_city_id")) // Join by city_id
     .write.format("noop").mode("overwrite").save()      // Test with a noop write

// Note Total Time Across All Tasks in the last stage (reducer stage): AQE's (with skew handling) one is slightly better .

// COMMAND ----------

sc.setJobDescription("Step F: SHJ Join - OOM")

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

spark.conf.set("spark.sql.shuffle.partitions", 80)           // Lowering number of shuffle partitions to artificially get bigger partition sizes

val trxDF1 = spark
  .read.format("delta").load(trxPath)                        // Load the transactions table
  .hint("SHUFFLE_HASH")                                      // Enforcing SHUFFLE_HASH using a join hint

val trxDF2 = spark.read.format("delta").load(trxPath)         // Load the transactions table again

trxDF1.join(trxDF2, trxDF1("z_city_id") === trxDF2("z_city_id")) // Join a big table with a big table
     .write.format("noop").mode("overwrite").save()          // Test with a noop write

// Result - "Error: Caused by: There is not enough memory to build the hash map"
// It will work fine with Photon which can spill to disk when doing SHJ (preferred join method over SMJ in Photon).