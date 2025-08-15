// Databricks notebook source
sc.setJobDescription("Step A: Basic initialization")
import org.apache.spark.sql.functions._

// Disable the Delta Cache (reduce side affects)
spark.conf.set("spark.databricks.io.cache.enabled", "false") 

// The city with the mostest
val targetCity = 2063810344

// COMMAND ----------

sc.setJobDescription("Step B: Establish a baseline")

// Not optimized for any type of filtering
val deltaPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.delta"

spark
  .read.format("delta").load(deltaPath)          // Load the delta table
  .filter($"city_id" === targetCity)             // Filter by target city
  .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step C: Partitioned by city")

// Partitioned on disk by city_id
val parPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-par_city.delta"

spark.read.format("delta").load(parPath)            // Load the partitioned, delta table
     .filter($"p_city_id" === targetCity)           // Filter by target city
     .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step D: Z-Ordered by city")

// Delta table Z-Ordered by city_id
val zoPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-zo_city.delta"

spark.read.format("delta").load(zoPath)             // Load the z-ordered, delta table
     .filter($"z_city_id" === targetCity)           // Filter by target city
     .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step E: Bucketed by city")

// Parquet file previously bucketed by city_id into 400 buckets
val bucketPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-bkt_city_400.parquet"

val tableName = "transactions_bucketed"               // The name of our table
spark.sql(s"CREATE DATABASE IF NOT EXISTS dbacademy") // Create the database
spark.sql(s"USE dbacademy")                           // Use the database
spark.sql(s"DROP TABLE IF EXISTS $tableName")         // Drop the table if it already exists

// Recate the buckted table from the provided files
spark.sql(s"""
  CREATE TABLE $tableName(transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,2), b_city_id integer)
  USING parquet 
  CLUSTERED BY(b_city_id) INTO 400 BUCKETS
  OPTIONS(PATH '$bucketPath')
""")

spark.read.table(tableName)                         // Load the bucketed table
     .filter($"b_city_id" === targetCity)           // Filter by target city
     .write.format("noop").mode("overwrite").save() // Execute a noop write to test