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

// Dataset has about 791 partions, use 832 to maintain factor of cores
spark.conf.set("spark.sql.shuffle.partitions", 832)

// Disable IO cache so as to minimize side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

// Location of our two datasets
val ctyPath = "dbfs:/mnt/training/global-sales/cities/all.delta"
val trxPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-par_city.delta"

// COMMAND ----------

sc.setJobDescription("Step B: Establish a baseline")

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

spark.read.format("delta").load(ctyPath)            // Load the city table
     .write.format("noop").mode("overwrite").save() // Test with a noop write

spark.read.format("delta").load(trxPath)            // Load the transactions table
     .write.format("noop").mode("overwrite").save() // Test with a noop write


// COMMAND ----------

sc.setJobDescription("Step C: Standard, Spilling, Join w/Skew")

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

val ctyDF = spark.read.format("delta").load(ctyPath)    // Load the city table

val trxDF = spark.read.format("delta").load(trxPath)    // Load the transactions table

val resultsDF = trxDF
  .join(ctyDF, ctyDF("city_id") === trxDF("p_city_id")) // Join by city_id

resultsDF.write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step D: Skew-Join Hint")

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

val ctyDF = spark.read.format("delta").load(ctyPath)    // Load the city table

val trxDF = spark
  .read.format("delta").load(trxPath)                   // Load the transactions table
  .hint("skew", "p_city_id")                            // Required to avoid Spill and/or Executor-OOM

val resultsDF = trxDF
  .join(ctyDF, $"city_id" === $"p_city_id")             // Join by city_id

resultsDF.write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step E: AQE Join")

// Enable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", true)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)

val ctyDF = spark.read.format("delta").load(ctyPath)    // Load the city table

val trxDF = spark
  .read.format("delta").load(trxPath)                   // Load the transactions table
  //.hint("skew", "p_city_id")                          // Not required with AQE

val resultsDF = trxDF
  .join(ctyDF, $"city_id" === $"p_city_id")             // Join by city_id

resultsDF.write.format("noop").mode("overwrite").save() // Test with a noop write