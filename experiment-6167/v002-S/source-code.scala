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
// MAGIC     <td>**64**</td>
// MAGIC     <td>**4096 cores**</td>
// MAGIC     <td>**31232 GB**</td>
// MAGIC   </tr>
// MAGIC </table>

// COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")

import org.apache.spark.sql.functions._

// Disable the advanced features that might alter our final result
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", false)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", false)

// Disable IO Cache to avoid side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

// Disable broadcasting to preclude unwanted optimizations
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

// COMMAND ----------

sc.setJobDescription("Step B: Establish a baseline")

// We start with 8192 cores, finish with 8192
spark.conf.set("spark.sql.shuffle.partitions", 8192)

// Read in our cities and in 1TB transactions table
val ctyDF = spark.read.format("delta").load("dbfs:/mnt/training/global-sales/cities/all.delta")              
val trxDF = spark.read.format("delta").load("dbfs:/mnt/training/global-sales/transactions/2011-to-2018-1tb.delta")              

trxDF.join(ctyDF, "city_id")                        // Join the two tables
     .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step C: Register our bucketed data")

// Buckets require tables and a database... setting those up
spark.sql("create database if not exists dbacademy")
spark.sql("use dbacademy")

val buckets = 4096 // Predetermined for us

// The path to our two bucketed tables
val ctyPath = s"dbfs:/mnt/training/global-sales/cities/all_bkt-city-$buckets.parquet" 
val trxPath = s"dbfs:/mnt/training/global-sales/transactions/2011-to-2018-1tb-bkt_trx_$buckets.parquet" 

val ctyTableName = "cities_bucketed"
val trxTableName = "transactions_bucketed"

// Create the cities table from existing files
spark.sql(f"drop table if exists $ctyTableName")
spark.sql(s"""
  CREATE TABLE $ctyTableName(b_city_id INT, city STRING, state STRING, state_abv STRING, country STRING)
  USING parquet 
  CLUSTERED BY(b_city_id) INTO $buckets BUCKETS
  OPTIONS(PATH '$ctyPath')
""")

// Create the transactions table from existing files
spark.sql(f"drop table if exists $trxTableName")
spark.sql(s"""
  CREATE TABLE $trxTableName(transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,2), b_city_id integer)
  USING parquet 
  CLUSTERED BY(b_city_id) INTO $buckets BUCKETS
  OPTIONS(PATH '$trxPath')
""")

// COMMAND ----------

sc.setJobDescription("Step D: Bucketing in Action")

// Partitions will equal buckets - we don't actually need to set
spark.conf.set("spark.sql.shuffle.partitions", buckets)

val ctyBktDF = spark.read.table(ctyTableName)       // Read in our bucketed cities table
val trxBktDF = spark.read.table(trxTableName)       // Read in our bucketed transactions table

trxBktDF.join(ctyBktDF, "b_city_id")                // Join the two tables
     .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

spark.sql(s"describe extended $trxTableName").show(25, false)

// COMMAND ----------

spark.sql(s"describe extended $ctyTableName").show(25, false)

// COMMAND ----------

spark.sessionState.catalog.getTableMetadata(org.apache.spark.sql.catalyst.TableIdentifier(trxTableName)).bucketSpec.foreach(println)

// COMMAND ----------

spark.sessionState.catalog.getTableMetadata(org.apache.spark.sql.catalyst.TableIdentifier(ctyTableName)).bucketSpec.foreach(println)

// COMMAND ----------

// Clean up our temp files
spark.sql(s"DROP TABLE IF EXISTS $ctyTableName");
spark.sql(s"DROP TABLE IF EXISTS $trxTableName");