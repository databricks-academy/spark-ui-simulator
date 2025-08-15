// Databricks notebook source
import org.apache.spark.sql.functions._

spark.conf.set("spark.sql.files.maxPartitionBytes", "256m")
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.databricks.io.cache.enabled", false)

val ctyTable = "cities_bucketed"
val ctyPath = s"dbfs:/mnt/training/global-sales/cities/all_bkt_city_400.parquet" 

val trxTable = "transactions_bucketed"
val trxPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-bkt_city_400.parquet"

// COMMAND ----------

// Configure the bucketing tables from existing datasets
val numBuckets = 100 // Predetermined
spark.sql(s"CREATE DATABASE IF NOT EXISTS dbacademy")
spark.sql(s"USE dbacademy")

// Setup the bucketed transactions table
spark.sql(s"DROP TABLE IF EXISTS $trxTable")
spark.sql(s"""CREATE TABLE $trxTable(transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,2), b_city_id integer)
              USING parquet CLUSTERED BY(b_city_id) INTO $numBuckets BUCKETS OPTIONS(PATH '$trxPath')""")

// Setup the bucketed cities table
spark.sql(s"DROP TABLE IF EXISTS $ctyTable")
spark.sql(s"""CREATE TABLE $ctyTable(b_city_id integer, city string, state string, state_abv string, country string)
              USING parquet CLUSTERED BY(b_city_id) INTO $numBuckets BUCKETS OPTIONS(PATH '$ctyPath')""")

// COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)

val ctyDF = spark.read.table(ctyTable)
val trxDF = spark.read.table(trxTable)
  .hint("skew", "b_city_id") // Required to avoid Executor-OOM

trxDF.join(ctyDF, ctyDF("b_city_id") === trxDF("b_city_id")).foreach(_=>())

// COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)

val ctyDF = spark.read.table(ctyTable)
val trxDF = spark.read.table(trxTable)
  // .hint("skew", "b_city_id")

trxDF.join(ctyDF, ctyDF("b_city_id") === trxDF("b_city_id")).foreach(_=>())

// COMMAND ----------


