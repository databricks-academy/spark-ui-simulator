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

// Disable the Delta IO Cache (reduce side affects)
spark.conf.set("spark.databricks.io.cache.enabled", "false")               

// Source directory for this experiment's dataset
val sourceDir = s"dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-par_year.delta" 

// COMMAND ----------

sc.setJobDescription("Step B: Default Shuffle Partitions")

spark
  .read.format("delta").load(sourceDir)
  .orderBy($"trx_id")
  .groupBy($"city_id").count
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step C: 832 Shuffle Partitions")

// We started with 825, finish with factor of cores
spark.conf.set("spark.sql.shuffle.partitions", 832)

// Enable AQE features to automatically coalesce partitions. This experiment reproduces cases where you are running on DBRs older than 7.3 or you manually disable AQE.
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

spark
  .read.format("delta").load(sourceDir)
  .orderBy($"trx_id")
  .groupBy($"city_id").count
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step D: 900 Shuffle Partitions")

// Random guess of a purposely higher number
spark.conf.set("spark.sql.shuffle.partitions", 900)

// Enable AQE features to automatically coalesce partitions
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)

spark
  .read.format("delta").load(sourceDir)
  .orderBy($"trx_id")
  .groupBy($"city_id").count
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step E: auto Shuffle Partitions")

// Random guess of a purposely higher number
spark.conf.set("spark.sql.shuffle.partitions", "auto")

// Enable AQE features to automatically coalesce partitions
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)

spark
  .read.format("delta").load(sourceDir)
  .orderBy($"trx_id")
  .groupBy($"city_id").count
  .write.format("noop").mode("overwrite").save()