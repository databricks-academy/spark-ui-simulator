// Databricks notebook source
import org.apache.spark.sql.functions._

// Disable cache to avoid side affects
spark.conf.set("spark.databricks.io.cache.enabled", "false")

val citiesDF = spark.read.format("delta").load("dbfs:/mnt/training/global-sales/cities/all.delta")

val transactionsDF = spark
  .read.format("delta")
  .load("dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-zo_city.delta")
  .hint("skew", "z_city_id") // Required to avoid Executor OOM

// COMMAND ----------

// 256mb partitions
spark.conf.set("spark.sql.files.maxPartitionBytes", "256m")
spark.conf.set("spark.sql.shuffle.partitions", 400)

transactionsDF
  .join(citiesDF, $"city_id" === $"z_city_id")
  .foreach(_=> ())

// COMMAND ----------

// 1024mb partitions
spark.conf.set("spark.sql.files.maxPartitionBytes", "1024m")
spark.conf.set("spark.sql.shuffle.partitions", 100)

transactionsDF
  .join(citiesDF, $"city_id" === $"z_city_id")
  .foreach(_=> ())

// COMMAND ----------

// MAGIC %run ./All-Done-Remove-Me
