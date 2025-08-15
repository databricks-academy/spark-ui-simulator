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

// Factor of 8 cores and greater than the expected 825 partitions
spark.conf.set("spark.sql.shuffle.partitions", 832)

// Disabled to avoid side effects
spark.conf.set("spark.databricks.io.cache.enabled", false) 

// Location of our datasets
val ctyPath = "dbfs:/mnt/training/global-sales/cities/all.delta"
val trxPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.delta"

// COMMAND ----------

sc.setJobDescription("Step B: How skewed?")

// How skewed is our data?
val visualizeDF = spark
  .read.format("delta").load(trxPath)
  .groupBy("city_id").count
  .orderBy($"count")

display(visualizeDF)

// COMMAND ----------

sc.setJobDescription("Step C: Establish a baseline")

// Ensure that AQE is disabled
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)

val ctyDF = spark.read.format("delta").load(ctyPath)  // Load the city table

val trxDF = spark.read.format("delta").load(trxPath)  // Load the transactions table

trxDF
  .join(ctyDF, ctyDF("city_id") === trxDF("city_id")) // Join by city_id
  .write.format("noop").mode("overwrite").save()      // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step D: Join with skew hint")

// Ensure that AQE is disabled
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)

val ctyDF = spark.read.format("delta").load(ctyPath)  // Load the city table

val trxDF = spark
  .read.format("delta").load(trxPath)                 // Load the transactions table
  .hint("skew", "city_id")                            // Required to avoid Executor-OOM and/or Spill

trxDF
  .join(ctyDF, ctyDF("city_id") === trxDF("city_id")) // Join by city_id
  .write.format("noop").mode("overwrite").save()      // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step E: Join with AQE")

// Enable AQE and the adaptive Skew Join
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)

// The default is 64 MB, but in this case we want to maintain 128m partitions
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")

val ctyDF = spark.read.format("delta").load(ctyPath)  // Load the cities table

val trxDF = spark
  .read.format("delta").load(trxPath)                 // Load the transactions table
  //.hint("skew", "city_id")                          // Not required with AQE's spark.sql.adaptive.skewedJoin

trxDF
  .join(ctyDF, ctyDF("city_id") === trxDF("city_id")) // Join by city_id
  .write.format("noop").mode("overwrite").save()      // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step F-1: Salted-Skew, saltDF")

spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)

// Too large - unnecissary overhead in the join and crossjoin
// Too small - skewed partition is not split up enough
// This value was selected after much experimentation
val skewFactor = 7

val saltDF = spark.range(skewFactor).toDF("salt")

// COMMAND ----------

sc.setJobDescription("Step F-2: Salted-Skew, ctySaltedDF")

// Post cross-join, we will be at ~865 MB (experimentation - see exchagne data size)
// 128 MB is Spark's safe, default partion size.
val partitions = Math.ceil(865 / 128d).toInt 

val ctySaltedDF = spark
  .read.format("delta").load(ctyPath)                // Load the delta table
  .repartition(partitions)                           // Pre-emptively avoiding spill post cross-join
  .crossJoin(saltDF)                                 // Cross join with saltDF
  .withColumn("salted_city_id",                      // Add the new column "salted_city_id"
              concat($"city_id", lit("_"), $"salt")) // Concatinate "city_id" and "salt"
  .drop("salt")                                      // Drop the now unused column "salt"

ctySaltedDF.printSchema

// COMMAND ----------

sc.setJobDescription("Step F-3: Salted-Skew, trxSaltedDF")

val trxSaltedDF = spark
  .read.format("delta").load(trxPath)                         // Load the delta table
  .withColumn("salt", (lit(skewFactor) * rand()).cast("int")) // Create a random "salt" column
  .withColumn("salted_city_id",                               // Add the new column "salted_city_id"
              concat(col("city_id"), lit("_"), col("salt")))  // Concatinate "city_id" and "salt"
  .drop("salt")                                               // Drop the now unused column "salt"

trxSaltedDF.printSchema

// COMMAND ----------

sc.setJobDescription("Step F-4: Salted-Skew, the join")

trxSaltedDF
  .join(ctySaltedDF, ctySaltedDF("salted_city_id") === trxSaltedDF("salted_city_id")) // Join by salted_city_id
  .write.format("noop").mode("overwrite").save()                                      // Execute a noop write to test