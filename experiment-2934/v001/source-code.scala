// Databricks notebook source
import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.files.maxPartitionBytes", "1280m") // 1.25gb, full read of part files
spark.conf.set("spark.databricks.io.cache.enabled", "false") // Disable the Delta IO Cache (reduce side affects)

// The city with the mostest
val targetCity = 2063810344

// COMMAND ----------

// Flat file (not optimized for filter)
val deltaPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.delta"

spark.read.format("delta").load(deltaPath)
     .filter($"city_id" === targetCity)
     .foreach(_=> ())

// COMMAND ----------

// Partitioned by city
val parPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-par_city.delta"

spark.read.format("delta").load(parPath)
     .filter($"p_city_id" === targetCity)
     .foreach(_=> ())

// COMMAND ----------

// Z-Ordered by city
val zoPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-zo_city.delta"

spark.read.format("delta").load(zoPath)
     .filter($"z_city_id" === targetCity)
     .foreach(_=> ())

// COMMAND ----------

// Bucketed by city
val numBuckets = 400 // Predetermined
val tableName = "transactions_bucketed"
val bucketPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-bkt_city_400.parquet"

spark.sql(s"CREATE DATABASE IF NOT EXISTS dbacademy")
spark.sql(s"USE dbacademy")
spark.sql(s"DROP TABLE IF EXISTS $tableName")

spark.sql(s"""
  CREATE TABLE $tableName(transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,2), b_city_id integer)
  USING parquet 
  CLUSTERED BY(b_city_id) INTO $numBuckets BUCKETS
  OPTIONS(PATH '$bucketPath')
""")

spark.read.table(tableName)
     .filter($"b_city_id" === targetCity)
     .foreach(_=> ())

// COMMAND ----------

// MAGIC %run ./All-Done-Remove-Me
