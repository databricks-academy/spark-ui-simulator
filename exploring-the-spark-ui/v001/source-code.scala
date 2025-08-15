// Databricks notebook source
// Step A |
val dataSourcePath = s"dbfs:/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean"

display( dbutils.fs.ls(dataSourcePath) )

// COMMAND ----------

// Step B |
val initialDF = spark                                                       
  .read                                                                     
  .parquet(dataSourcePath)   
  .cache()

initialDF.foreach(x => ()) 

// COMMAND ----------

// Step C |
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

// Step D |
someDF.take(total)

// COMMAND ----------

// Step E |
var bigDF = initialDF

for (i <- 0 to 6) {
  bigDF = bigDF.union(bigDF).repartition(sc.defaultParallelism)
}

bigDF.foreach(_=>())

// COMMAND ----------


