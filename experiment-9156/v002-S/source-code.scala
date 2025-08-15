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
// MAGIC     <td>**32**</td>
// MAGIC     <td>**2048 cores**</td>
// MAGIC     <td>**15616 GB**</td>
// MAGIC   </tr>
// MAGIC </table>

// COMMAND ----------

sc.setJobDescription("Step A-1: Basic initialization")

import org.apache.spark.sql.functions._

// Disable IO cache so as to minimize side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

val trxPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018_int-only_1tb.delta"

// COMMAND ----------

sc.setJobDescription("Step A-2: Utility Function")

def partitionStats(df:org.apache.spark.sql.Dataset[Row], maximum:Long):Unit = {
  val results = df.mapPartitions(it => Array(it.size).iterator).collect().map(_.toLong)
  
  val overCount = results.filter(_>maximum).length
  val under = results.filter(r => r>0 && r<=maximum)
  
  var html = f"""
  <h2 style="margin:0">Partition Statistics</h2>
  <table>
    <tr><td>Empty:</td>    <td style="text-align:right">${results.filter(_==0).length}%,d</td></tr>
    <tr><td>Not Empty:</td><td style="text-align:right">${results.filter(_!=0).length}%,d</td></tr>
    <tr><td style="border-top:1px solid black; padding-bottom:10px">Total:</td><td style="text-align:right; border-top:1px solid black; padding-bottom:10px">${results.length}%,d</td></tr>

    <tr><td>Over Count:</td>  <td style="text-align:right">${overCount}%,d</td></tr>
    <tr><td style="padding-bottom:10px">Largest:</td><td style="text-align:right; padding-bottom:10px">${results.max}%,d</td></tr>

    <tr><td>Under Count:</td><td style="text-align:right">${under.length}%,d</td></tr>
    <tr><td>Under Avg:</td>  <td style="text-align:right">${under.sum/under.length}%,d</td></tr>
    <tr><td>Under Sum:</td>  <td style="text-align:right">${under.sum}%,d</td></tr>
  </table> """
  
  html = html + f"<h3 style='margin:0; margin-top:1em'>Partitions with more than $maximum%,d records</h3><table>" 
  if (overCount > 0) {
    for ( (r,i) <- results.filter(_>maximum).view.zipWithIndex)  
      html = html + f"<tr><td>#$i: $r%,d</td></tr>"
  } else html = html + "<tr><td>-none found-</td></tr>"
  html = html + f"</table>"
  
  displayHTML(html)
}

// COMMAND ----------

sc.setJobDescription("Step B: No skew")

val resultsDF = spark
  .read.format("delta").load(trxPath) // Load the transactions table
  .groupBy("city_id").count           // Group by city_id and count

display(resultsDF)

// COMMAND ----------

sc.setJobDescription("Step C: General Statistics")

val records = spark.read.format("delta").load(trxPath).count
val partitions = spark.read.format("delta").load(trxPath).rdd.getNumPartitions
val cities = spark.read.format("delta").load(trxPath).select("city_id").distinct.count

// Keep the number of partitions we started with.
spark.conf.set("spark.sql.shuffle.partitions", partitions)

displayHTML(f"""
<h2 style="margin:0">General Statistics</h2>
<table>
  <tr><td>Cities: </td>    <td style="text-align:right;">$cities%,d</td></tr>
  <tr><td>Records: </td>   <td style="text-align:right;">$records%,d</td></tr>
""")

// COMMAND ----------

sc.setJobDescription("Step D-1: Before - Records/Partition")

val beforeDF = spark.read.format("delta").load(trxPath)

val resultsBeforeDF = beforeDF
  .mapPartitions(it => Array(it.size).iterator)
  .withColumn("spark_partition_id", spark_partition_id())
  .orderBy($"value".desc)

display(resultsBeforeDF)

// COMMAND ----------

sc.setJobDescription("Step D-2: Before - Stats")

partitionStats(beforeDF, 4100000)

// COMMAND ----------

sc.setJobDescription("Step E: After - Records/Partition")

val afterDF = spark
  .read.format("delta").load(trxPath)
  .repartition(partitions, $"city_id")

val resultsAfterDF = afterDF
  .mapPartitions(it => Array(it.size).iterator)
  .withColumn("spark_partition_id", spark_partition_id())
  .orderBy($"value".desc)

display(resultsAfterDF)

// COMMAND ----------

sc.setJobDescription("Step D-2: After - Stats")

partitionStats(afterDF, 134000000)