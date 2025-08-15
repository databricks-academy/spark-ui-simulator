// Databricks notebook source
import org.apache.spark.sql.functions._

spark.conf.set("spark.sql.files.maxPartitionBytes", "256m")
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.databricks.io.cache.enabled", false)

val ctyPath = "dbfs:/mnt/training/global-sales/cities/all.delta"
val trxPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-zo_city.delta"

// COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)

val ctyDF = spark
  .read.format("delta").load(ctyPath)
  .filter($"country" === "USA")

val trxDF = spark.read.format("delta").load(trxPath)

trxDF.join(ctyDF, $"city_id" === $"z_city_id").foreach(_=>())

// COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)

val ctyDF = spark
  .read.format("delta").load(ctyPath)
  .filter($"country" === "USA")

val trxDF = spark.read.format("delta").load(trxPath)

trxDF.join(ctyDF, $"city_id" === $"z_city_id").foreach(_=>())

// COMMAND ----------


