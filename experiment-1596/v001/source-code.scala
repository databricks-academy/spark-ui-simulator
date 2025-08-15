// Databricks notebook source
import org.apache.spark.sql.functions._

spark.conf.set("spark.sql.files.maxPartitionBytes", "256m")
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.databricks.io.cache.enabled", false)

val ctyPath = "dbfs:/mnt/training/global-sales/cities/all.delta"
val trxPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.delta"

// COMMAND ----------

// Join with skew hint
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)

val ctyDF = spark.read.format("delta").load(ctyPath)

val trxDF = spark
  .read.format("delta").load(trxPath)
  .hint("skew", "city_id") // Required to avoid Executor-OOM

trxDF.join(ctyDF, ctyDF("city_id") === trxDF("city_id")).foreach(_=>())

// COMMAND ----------

// Join with AQE
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)

val ctyDF = spark.read.format("delta").load(ctyPath)

val trxDF = spark
  .read.format("delta").load(trxPath)
  // Not required with spark.sql.adaptive.skewedJoin  
  // .hint("skew", "city_id")

trxDF.join(ctyDF, ctyDF("city_id") === trxDF("city_id")).foreach(_=>())

// COMMAND ----------

// Salting, Step #1
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)


val range = spark.read.format("delta").load(trxPath).select("city_id").distinct.count

// 35 MB * city-count / 128 MB
val partitions = (35 * range / 128 ).toInt

val saltDF = spark
  .range(range).toDF("salt")
  .repartition(partitions)

// COMMAND ----------

// Salting, Step #2
val ctySaltedDF = spark.read.format("delta").load(ctyPath)
  .repartition(partitions) 
  .crossJoin(saltDF)
  .withColumn("salted_city_id",concat(col("city_id"), lit("_"), col("salt")))
  .drop("salt")

ctySaltedDF.printSchema

// COMMAND ----------

// Salting Step #3
val trxSaltedDF = spark
  .read.format("delta").load(trxPath)
  .withColumn("salt", (lit(range) * rand()).cast("int"))
  .withColumn("salted_city_id", concat(col("city_id"), lit("_"), col("salt")))
  .drop("salt")

trxSaltedDF.printSchema

// COMMAND ----------

// Salted join, no AQE
trxSaltedDF.join(ctySaltedDF, ctySaltedDF("salted_city_id") === trxSaltedDF("salted_city_id")).foreach(_=>())

// COMMAND ----------

// MAGIC %run ./All-Done-Remove-Me
