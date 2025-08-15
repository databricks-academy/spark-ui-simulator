// Databricks notebook source
sc.setJobDescription("Step A: Basic initialization")

val dataSourcePath = s"dbfs:/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean"

dbutils.fs.ls(dataSourcePath).map(_.name).foreach(println)

// COMMAND ----------

sc.setJobDescription("Step B: Read and cache the initial DataFrame")

val initialDF = spark
  .read
  .parquet(dataSourcePath)
  .cache()

initialDF.foreach(x => ())

// COMMAND ----------

sc.setJobDescription("Step C: A bunch of random transformations")

import org.apache.spark.sql.functions.upper

val someDF = initialDF
  .withColumn("first", upper($"article".substr(0,1)) )
  .where( $"first".isin("A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z") )
  .groupBy($"project", $"first").sum()
  .drop("sum(bytes_served)")
  .orderBy($"first", $"project")
  .select($"first", $"project", $"sum(requests)".as("total"))
  .filter($"total" > 10000)

val total = someDF.count().toInt

// COMMAND ----------

sc.setJobDescription("Step D: Take N records")

val all = someDF.take(total)

// COMMAND ----------

sc.setJobDescription("Step E: Create a really big DataFrame")

var bigDF = initialDF

for (i <- 0 until 7) {
  bigDF = bigDF.union(bigDF).repartition(sc.defaultParallelism)
}

bigDF.write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step F: Streaming Job")

import org.apache.spark.sql.functions.window
spark.conf.set("spark.sql.shuffle.partitions", 8)

val dataPath = "dbfs:/mnt/training/definitive-guide/data/activity-data-stream.json"
val dataSchema = "Recorded_At timestamp, Device string, Index long, Model string, User string, _corrupt_record String, gt string, x double, y double, z double"

val streamingDF = spark
  .readStream
  .option("maxFilesPerTrigger", 1)
  .schema(dataSchema)
  .json(dataPath)
  .groupBy($"Device", window($"Recorded_At", "20 seconds"))
  .count
  .select($"window.start".as("start"), $"Device", $"count")

display(streamingDF, streamName = "Sample_Stream")

// COMMAND ----------

sc.setJobDescription("Step G: Stop stream after 30 sec")

Thread.sleep(1000*30)

for (stream <- spark.streams.active) {
    stream.stop()
}

// COMMAND ----------

