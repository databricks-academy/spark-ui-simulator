// Databricks notebook source
// Disable the Delta IO Cache (reduce side affects)
spark.conf.set("spark.databricks.io.cache.enabled", "false")               

// Source directory this experiment's dataset
val sourceDir = s"dbfs:/mnt/training/global-sales/solutions/1990-to-2009.parquet" 
val schema = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer, new_at timestamp"

val defaultSize = spark.conf.get("spark.sql.files.maxPartitionBytes")      // What is the maximum size of each spark-partition (default value)?
val files = dbutils.fs.ls(sourceDir).filter(_.name.endsWith(".parquet"))   // How many parquet files are in this directory?
val avgSize = (files.map(_.size).sum/files.size)/1024.0 / 1024.0 / 1024.0  // What is the avage size in GB of each file?

displayHTML(f"""
Max Partition Bytes: $defaultSize (default)<br/><br/>
File Count: ${files.size}<br/>
Avg Size: ${avgSize}%.02f GB<br/>
""")

// COMMAND ----------

// 1 x default
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728b")
spark.read.schema(schema).parquet(sourceDir).foreach(x => ())

// COMMAND ----------

// Induce an artificial delay to make
// each job more distinguishable
Thread.sleep(2*60*1000)

// COMMAND ----------

// 2 x default
spark.conf.set("spark.sql.files.maxPartitionBytes", "268435456b")
spark.read.schema(schema).parquet(sourceDir).foreach(x => ())

// COMMAND ----------

Thread.sleep(2*60*1000)

// COMMAND ----------

// 4 x default
spark.conf.set("spark.sql.files.maxPartitionBytes", "536870912b")
spark.read.schema(schema).parquet(sourceDir).foreach(x => ())

// COMMAND ----------

Thread.sleep(2*60*1000)

// COMMAND ----------

// 8 x default, ~1 GB
spark.conf.set("spark.sql.files.maxPartitionBytes", "1073741824b") 
spark.read.schema(schema).parquet(sourceDir).foreach(x => ())

// COMMAND ----------

Thread.sleep(2*60*1000)

// COMMAND ----------

// 16 x default
spark.conf.set("spark.sql.files.maxPartitionBytes", "2147483648b") 
spark.read.schema(schema).parquet(sourceDir).foreach(x => ())

// COMMAND ----------

// MAGIC %run ./All-Done-Remove-Me
