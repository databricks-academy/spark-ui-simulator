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
// MAGIC     <td>**16**</td>
// MAGIC     <td>**1024 cores**</td>
// MAGIC     <td>**7808 GB**</td>
// MAGIC   </tr>
// MAGIC </table>

// COMMAND ----------

sc.setJobDescription("Step A-1: Basic initialization")

import org.apache.spark.sql.functions._

// Disable the IO Cache to avoid side effects
spark.conf.set("spark.databricks.io.cache.enabled", "false")

// COMMAND ----------

sc.setJobDescription("Step A-2: Utility method to count files & directories")

def countFiles(name:String, df:org.apache.spark.sql.DataFrame, partitions:Integer=0):Unit = {
  sc.setJobDescription("Step " + name)
  
  val filesDF = df
    .select(input_file_name() as "path")
    .withColumn("file", substring_index($"path", "/", -1))
    .withColumn("directory", substring_index($"path", "/", partitions+6))

  filesDF.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)

  val recordCount = filesDF.count()
  val fileCount = filesDF.select("file").distinct.count
  val directoryCount = filesDF.select("directory").distinct.count

 filesDF.unpersist()

  displayHTML(f"""
  <table>
    <tr><td>Dataset:</td>    <td style="text-align:right; font-weight:bold">$name</td></tr>
    <tr><td>Records:</td>    <td style="text-align:right; font-weight:bold">$recordCount%,d</td></tr>
    <tr><td>Files:</td>      <td style="text-align:right; font-weight:bold">$fileCount%,d</td></td></tr>
    <tr><td>Directories:</td><td style="text-align:right; font-weight:bold">$directoryCount%,d</td></td></tr>
  """) 
}

// COMMAND ----------

sc.setJobDescription("Step B-1: 2013")

// Contains ~100 records per part file
val df2013 = spark.read.parquet("dbfs:/mnt/training/global-sales/transactions/2013.parquet")

// COMMAND ----------

// Contains ~100 records per part file
countFiles("B-2: 2013", df2013)

// COMMAND ----------

sc.setJobDescription("Step C-1: 2014")

// Partitioned by year and month
val df2014 = spark.read.parquet("dbfs:/mnt/training/global-sales/transactions/2014.parquet")

// COMMAND ----------

// Partitioned by year and month
countFiles("C-2: 2014", df2014, partitions=2)

// COMMAND ----------

sc.setJobDescription("Step D-1: 2015")

// Partitioned by year, month, day and hour
val df2015 = spark.read.parquet("dbfs:/mnt/training/global-sales/transactions/2015.parquet")

// COMMAND ----------

// Partitioned by year, month, day and hour
countFiles("D-2: 2015", df2015, partitions=4)

// COMMAND ----------

sc.setJobDescription("Step E-1: 2016")

// Partitioned by retailer_id
val df2016 = spark.read.parquet("dbfs:/mnt/training/global-sales/transactions/2016.parquet")

// COMMAND ----------

// Partitioned by retailer_id
countFiles("E-2: 2016", df2016, partitions=1)

// COMMAND ----------

sc.setJobDescription("Step F-1: 2017")

// Expected data types, no partitioning
val df2017 = spark.read.parquet("dbfs:/mnt/training/global-sales/transactions/2017.parquet")

// COMMAND ----------

// Expected data types, no partitioning
countFiles("F-2: 2017", df2017)

// COMMAND ----------

sc.setJobDescription("Step G-1: 2018")

// Partitioned by year and month, ~750 files month, 28639 files in November.
val df2018 = spark.read.parquet("dbfs:/mnt/training/global-sales/transactions/2018.parquet")

// COMMAND ----------

// Partitioned by year and month, ~750 files month, 28639 files in November.
countFiles("G-2: 2018", df2018, partitions=2)

// COMMAND ----------

sc.setJobDescription("Step H-1: 2015 w/Schema")

val schema2015 = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,18), city_id integer, year integer, month integer, day integer, hour integer"

// Partitioned by year, month, day and hour
val df2015WithSchema = spark
  .read.schema(schema2015)
  .parquet("dbfs:/mnt/training/global-sales/transactions/2015.parquet")

// COMMAND ----------

// Partitioned by year, month, day and hour
countFiles("H-2: 2015 w/Schema", df2015WithSchema, partitions=4)

// COMMAND ----------

sc.setJobDescription("Step I-1: 2015 as Table")

spark.sql("CREATE DATABASE IF NOT EXISTS dbacademy")
spark.sql("USE dbacademy")
spark.sql("DROP TABLE IF EXISTS 2015_Transactions")

spark.sql("CREATE TABLE 2015_Transactions USING parquet LOCATION 'dbfs:/mnt/training/global-sales/transactions/2015.parquet'")

spark.sql("MSCK repair table 2015_Transactions")

// COMMAND ----------

sc.setJobDescription("Step I-2: 2015 as Table")

// Partitioned by year, month, day and hour
val df2015AsTable = spark.read.table("2015_Transactions")

// COMMAND ----------

// Partitioned by year, month, day and hour
countFiles("I-3: 2015 as Table", df2015AsTable, partitions=4)

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS 2015_Transactions")

// COMMAND ----------

sc.setJobDescription("Step J: One last test")

spark.read.parquet("dbfs:/mnt/training/global-sales/transactions/2015.parquet") 
     .write.format("noop").mode("overwrite").save() // Execute a noop write to test