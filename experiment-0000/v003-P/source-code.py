# Databricks notebook source
sc.setJobDescription("Step A: Basic Initialization")

dataSourcePath = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/wikipedia/pagecounts/staging_parquet_en_only_clean"

[print(f.name) for f in dbutils.fs.ls(dataSourcePath)]

# COMMAND ----------

sc.setJobDescription("Step B: Read & Cache Page-Counts")

pagecounts_path = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/wikipedia/pagecounts/staging_parquet_en_only_clean"

initialDF = (spark
  .read
  .parquet(pagecounts_path)
  .cache()
)

# A do-nothing operation that simply materializes the cache
initialDF.write.format("noop").mode("overwrite").save()

# COMMAND ----------

sc.setJobDescription("Step C: Random Transformations")

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

sc.setJobDescription("Step E: Induce Spill w/Big DataFrame")

bigDF = initialDF

for i in range(0, 7):
  bigDF = bigDF.union(bigDF).repartition(sc.defaultParallelism)

bigDF.write.format("noop").mode("overwrite").save()

# COMMAND ----------

sc.setJobDescription("Step F: Streaming Job")

from pyspark.sql.functions import window, col
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

dataPath = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/definitive-guide/data/activity-data-stream.json"
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

sc.setJobDescription("Step G: Stop All Streams")
import time

# Let the stream run for 15 seconds
time.sleep(30) 

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

sc.setJobDescription("Step H: 100 GB Delta IO Cache")
from pyspark.sql.functions import xxhash64, col

# Source dataset is about ~800 partitions, 900 is safe
spark.conf.set("spark.sql.shuffle.partitions", 900)

transactions_df = spark.read.load("wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb.delta")

# Artificailly materialize DBIO cache
transactions_df.write.format("noop").mode("overwrite").save() 

# COMMAND ----------

sc.setJobDescription("Step I: 100 GB Join")

columns = filter(lambda f: not f == "city_id", transactions_df.columns)
for column in columns:
  transactions_df = transactions_df.withColumn(f"{column}-hash", xxhash64(column))

cities_df = spark.read.load("wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/cities/all.delta")

columns = filter(lambda f: not f == "city_id", cities_df.columns)
for column in columns:
  cities_df = cities_df.withColumn(f"{column}-hash", xxhash64(col(column)))

joined_df = (transactions_df
  .join(cities_df, "city_id")
  .withColumn("city_id-hash", xxhash64("city_id"))
  .orderBy("amount")
  .write.format("noop").mode("overwrite").save()
)