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
// MAGIC 
// MAGIC <p>
// MAGIC   Special Notes:
// MAGIC   <li>The dataset used for this experiment is too small to show a significant performance difference between the "standard" and "bucketed" joins.</li>
// MAGIC   <li>The dataset includes significant spew along the same index that we are bucketing by which compunds the skew's effect.</li>
// MAGIC   <li>Please see <a href="https://www.databricks.training/spark-ui-simulator/experiment-6167" target="_blank">Experiment #6167, Bucket Join</a> for a proper demonstration at terabyte scale.
// MAGIC </p>

// COMMAND ----------

sc.setJobDescription("Step A-1: Basic initialization")

import org.apache.spark.sql.functions._

val buckets = 800 // Predetermined
spark.conf.set("spark.sql.shuffle.partitions", buckets)

// Disable IO cache so as to minimize side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

val ctyTable = "cities_bucketed"
val ctyPath = f"dbfs:/mnt/training/global-sales/cities/all_bkt_city_$buckets.parquet" 

val trxTable = "transactions_bucketed"
val trxPath = f"dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-bkt_city_$buckets.parquet"

// COMMAND ----------

sc.setJobDescription("Step A-2: Configure buckets from existing datasets")

spark.sql(s"CREATE DATABASE IF NOT EXISTS dbacademy")
spark.sql(s"USE dbacademy")

// Setup the bucketed transactions table
spark.sql(s"DROP TABLE IF EXISTS $trxTable")
spark.sql(s"""CREATE TABLE $trxTable(transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,2), b_city_id integer)
              USING parquet CLUSTERED BY(b_city_id) INTO $buckets BUCKETS OPTIONS(PATH '$trxPath')""")

// Setup the bucketed cities table
spark.sql(s"DROP TABLE IF EXISTS $ctyTable")
spark.sql(s"""CREATE TABLE $ctyTable(b_city_id integer, city string, state string, state_abv string, country string)
              USING parquet CLUSTERED BY(b_city_id) INTO $buckets BUCKETS OPTIONS(PATH '$ctyPath')""")

// COMMAND ----------

sc.setJobDescription("Step B: Establish a baseline")

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

spark.read.table(ctyTable)                          // Load the city table
     .filter($"country".substr(0, 1).isin("A","B")) // Countries starting with A or B
     .write.format("noop").mode("overwrite").save() // Test with a noop write

spark.read.table(trxTable)                          // Load the transactions table
     .write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step C: Standard Join")

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

val ctyDF = spark
  .read.table(ctyTable)                                      // Load the city table
  .filter($"country".substr(0, 1).isin("A","B"))             // Countries starting with A or B

val trxDF = spark.read.table(trxTable)                       // Load the transactions table

trxDF.join(ctyDF, ctyDF("b_city_id") === trxDF("b_city_id")) // Join by city_id
     .write.format("noop").mode("overwrite").save()          // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step D: AQE Join")

// Enable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", true)    
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)

val ctyDF = spark
  .read.table(ctyTable)                                      // Load the city table
  .filter($"country".substr(0, 1).isin("A","B"))             // Countries starting with A or B

val trxDF = spark.read.table(trxTable)                       // Load the transactions table

trxDF.join(ctyDF, ctyDF("b_city_id") === trxDF("b_city_id")) // Join by city_id
     .write.format("noop").mode("overwrite").save()          // Test with a noop write