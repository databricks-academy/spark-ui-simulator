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
// MAGIC     <td>**2**</td>
// MAGIC     <td>**1024 cores**</td>
// MAGIC     <td>**7808 GB**</td>
// MAGIC   </tr>
// MAGIC </table>

// COMMAND ----------

// MAGIC %md # Bucket the Transactions & Cities Tables

// COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")

spark.conf.set("spark.databricks.io.cache.enabled", true)

// Setup the database that these tables will exists in
spark.sql("create database if not exists dbacademy")
spark.sql("use dbacademy")

// Locations of our orignal datasets
val trxReadDir = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-1tb.delta"
val ctyReadDir = "dbfs:/mnt/training/global-sales/cities/all.delta"

// COMMAND ----------

sc.setJobDescription("Step B: Showcase the non-bucketed join")

// We start with 8192, finsih with 8192
spark.conf.set("spark.sql.shuffle.partitions", 8192)

val trxStandardDF = spark.read.format("delta").load(trxReadDir)
val ctyStandardDF = spark.read.format("delta").load(ctyReadDir)

trxStandardDF
  .join(ctyStandardDF, "city_id")
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step C: Calculate bucket count")

val sizeOnDiskMB = dbutils.fs.ls(trxReadDir).filter(_.path.endsWith(".parquet")).map(_.size).sum / 1024 / 1024 / 1024
val targetFileSizeMB = 256
val scaleFactor = sizeOnDiskMB.toDouble / targetFileSizeMB.toDouble
val buckets = Math.ceil(sizeOnDiskMB * scaleFactor).toInt

displayHTML(f"""
<table>
  <tr><td>Size On Disk:</td>     <td style="font-weight:bold; text-align: right">${sizeOnDiskMB}%,d MB</td></tr
  <tr><td>Target File Size:</td> <td style="font-weight:bold; text-align: right">${targetFileSizeMB}%,d MB</td></tr
  <tr><td>Scale Factor:</td>     <td style="font-weight:bold; text-align: right">${scaleFactor}%,.2f MB</td></tr
  <tr><td>Buckets:</td>          <td style="font-weight:bold; text-align: right">${buckets}%,d MB</td></tr
</table>
""")

// COMMAND ----------

sc.setJobDescription("Step D: Setup the transactions table")

// Setup the destination table and file paths
val trxTableName = s"transactions_1t_bucketed_$buckets"
val trxWriteDir = f"dbfs:/tmp/dbacademy/global-sales/transactions/2011-to-2018-1tb-bkt_trx_$buckets.parquet"

spark.sql(s"DROP TABLE IF EXISTS $trxTableName"); // Drop the table from previous runs
val trxDeleted = dbutils.fs.rm(trxWriteDir, true) // Remove any files left behind

// COMMAND ----------

sc.setJobDescription("Step E: Create the bucketed transactions table")

spark
  .read.format("delta").load(trxReadDir)          // Load the transactions table
  .withColumnRenamed("city_id", "b_city_id")      // Advertise our features with a column rename
  .repartition(buckets, $"b_city_id")             // Relocate each city into the correct partition (mitigates tiny files)
  .write                          
  .bucketBy(buckets, "b_city_id")                 // Bucket by N buckets and b_city_id
  .sortBy("b_city_id")                            // Sort the data (within each bucket)
  .option("path", trxWriteDir)                    // Specify the location of the "managed" table
  .saveAsTable(trxTableName)                      // Execute the operation

// COMMAND ----------

sc.setJobDescription("Step F: Show the transaction files")

display( dbutils.fs.ls(trxWriteDir) )

// COMMAND ----------

sc.setJobDescription("Step G: Setup the cities table")

// Setup the destination table and file paths
val ctyTableName = s"cities_bucketed_$buckets"
val ctyWriteDir = f"dbfs:/tmp/dbacademy/global-sales/cities/all_bkt-city-$buckets.parquet"

spark.sql(s"DROP TABLE IF EXISTS $ctyTableName"); // Drop the table from previous runs
val ctyDeleted = dbutils.fs.rm(ctyWriteDir, true) // Remove any files left behind

// COMMAND ----------

sc.setJobDescription("Step H: Create the bucketed transactions table")

// Setup the destination table and file paths
val ctyTableName = s"cities_bucketed_$buckets"
val ctyWriteDir = f"dbfs:/tmp/dbacademy/global-sales/cities/all_bkt-city-$buckets.parquet"

spark.sql(s"DROP TABLE IF EXISTS $ctyTableName"); // Drop the table from previous runs
val ctyDeleted = dbutils.fs.rm(ctyWriteDir, true) // Remove any files left behind

spark
  .read.format("delta").load(ctyReadDir)          // Load the cities table
  .withColumnRenamed("city_id", "b_city_id")      // Advertise our features with a column rename
  .coalesce(1)                                    // ~30MB table, produces $buckets tiny files
  .write
  .bucketBy(buckets, "b_city_id")                 // Bucket by N buckets and b_city_id
  .sortBy("b_city_id")                            // Sort the data (within each bucket)
  .option("path", ctyWriteDir)                    // Specify the location of the "managed" table
  .saveAsTable(ctyTableName)                      // Execute the operation

// COMMAND ----------

sc.setJobDescription("Step I: Show the cities files")

display( dbutils.fs.ls(ctyWriteDir) )

// COMMAND ----------

sc.setJobDescription("Step J: Showcase the join")

val trxBucketDF = spark.read.table(trxTableName)
val ctyBucketDF = spark.read.table(ctyTableName)

trxBucketDF
  .join(ctyBucketDF, "b_city_id")
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

// MAGIC %md
// MAGIC # Inspect the tables we just created

// COMMAND ----------

display( spark.catalog.listTables.where($"name" contains "bucket") )

// COMMAND ----------

spark.sql(s"describe extended $trxTableName").show(25, false)

// COMMAND ----------

spark.sql(s"describe extended $ctyTableName").show(25, false)

// COMMAND ----------

spark.sessionState.catalog.getTableMetadata(org.apache.spark.sql.catalyst.TableIdentifier(trxTableName)).bucketSpec.foreach(println)

// COMMAND ----------

spark.sessionState.catalog.getTableMetadata(org.apache.spark.sql.catalyst.TableIdentifier(ctyTableName)).bucketSpec.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC # How To Register A Bucketed Dataset As A Table

// COMMAND ----------

val newTrxTableName = trxTableName+"_test"

spark.sql(s"drop table if exists $newTrxTableName")

spark.sql(s"""
  CREATE TABLE $newTrxTableName(transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,2), b_city_id integer)
  USING parquet 
  CLUSTERED BY(b_city_id) INTO $buckets BUCKETS
  OPTIONS(PATH '$trxWriteDir')
""")

// COMMAND ----------

display( spark.read.table(newTrxTableName) )

// COMMAND ----------

val newCtyTableName = ctyTableName+"_test"

spark.sql(s"drop table if exists $newCtyTableName")

spark.sql(s"""
  CREATE TABLE $newCtyTableName(b_city_id INT, city STRING, state STRING, state_abv STRING, country STRING)
  USING parquet 
  CLUSTERED BY(b_city_id) INTO $buckets BUCKETS
  OPTIONS(PATH '$ctyWriteDir')
""")

// COMMAND ----------

display( spark.read.table(newCtyTableName) )

// COMMAND ----------

// MAGIC %md
// MAGIC # Remove All Tables

// COMMAND ----------

spark.sql(s"drop table if exists $trxTableName")
spark.sql(s"drop table if exists $ctyTableName")

spark.sql(s"drop table if exists $newTrxTableName")
spark.sql(s"drop table if exists $newCtyTableName")