# Databricks notebook source
sc.setJobDescription("Step A: Basic initialization")

dataSourcePath = "dbfs:/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean"

[print(f.name) for f in dbutils.fs.ls(dataSourcePath)] 

# COMMAND ----------

sc.setJobDescription("Step B: Read and cache the initial DataFrame")

initialDF = (spark
  .read
  .parquet(dataSourcePath)
  .cache()
)
initialDF.foreach(lambda x: None)

# COMMAND ----------

sc.setJobDescription("Step C: A bunch of random transformations")

from pyspark.sql.functions import col, upper

someDF = (initialDF
  .withColumn("first", upper(col("article").substr(0,1)) )
  .where( col("first").isin("A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z") )
  .groupBy(col("project"), col("first")).sum()
  .drop("sum(bytes_served)")
  .orderBy(col("first"), col("project"))
  .select(col("first"), col("project"), col("sum(requests)").alias("total"))
  .filter(col("total") > 10000)
)
total = someDF.count()

# COMMAND ----------

sc.setJobDescription("Step D: Take N records")

all = someDF.take(total)

# COMMAND ----------

sc.setJobDescription("Step E: Create a really big DataFrame")

bigDF = initialDF

for i in range(0, 7):
  bigDF = bigDF.union(bigDF).repartition(sc.defaultParallelism)

bigDF.write.format("noop").mode("overwrite").save()

# COMMAND ----------

sc.setJobDescription("Step F: Streaming Job")

from pyspark.sql.functions import window, col
spark.conf.set("spark.sql.shuffle.partitions", 8)

dataPath = "dbfs:/mnt/training/definitive-guide/data/activity-data-stream.json"
dataSchema = "Recorded_At timestamp, Device string, Index long, Model string, User string, _corrupt_record String, gt string, x double, y double, z double"

streamingDF = (spark
  .readStream
  .option("maxFilesPerTrigger", 1)
  .schema(dataSchema)
  .json(dataPath)
  .groupBy(col("Device"), window(col("Recorded_At"), "20 seconds"))
  .count()
  .select(col("window.start").alias("start"), col("Device"), col("count"))
)
display(streamingDF, streamName = "Sample_Stream")

# COMMAND ----------

sc.setJobDescription("Step G: Stop stream after 30 sec")

import time
time.sleep(30)

for stream in spark.streams.active:
    stream.stop()


# COMMAND ----------

