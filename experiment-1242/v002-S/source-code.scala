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

sc.setJobDescription("Step A-1: Basic initialization")

// Disable the Delta IO Cache (reduce side affects)
spark.conf.set("spark.databricks.io.cache.enabled", "false")               

// What is the maximum size of each spark-partition (default value)?
val defaultMaxPartitionBytes = spark.conf.get("spark.sql.files.maxPartitionBytes").replace("b","").toLong

// What is the cost in bytes for each file (default value)?
val openCostInBytes = spark.conf.get("spark.sql.files.openCostInBytes").toLong

displayHTML(f"""<table>
  <tr><td>Max Partition Bytes:</td><td><b>$defaultMaxPartitionBytes</b> Bytes or <b>${defaultMaxPartitionBytes/1024/1024}</b> MB</td></tr>
  <tr><td>Open Cost In Bytes: </td><td><b>$openCostInBytes</b> Bytes or <b>${openCostInBytes/1024/1024}</b> MB</td></tr>
</table>""")

// COMMAND ----------

sc.setJobDescription("Step A-2: Utility Function")

def predictNumPartitions(files:Seq[com.databricks.backend.daemon.dbutils.FileInfo]) = {
  
  val openCost:Long = spark.conf.get("spark.sql.files.openCostInBytes").toLong
  val maxPartitionBytes:Long = spark.conf.get("spark.sql.files.maxPartitionBytes").replace("b","").toLong

  val actualBytes = files.map(_.size).sum                            // Total size of the dataset on disk 
  val paddedBytes = actualBytes + (files.length * openCost)          // Final size with padding from openCost
  
  val bytesPerCore:Long = (paddedBytes/sc.defaultParallelism).toLong // The number of bytes per core
  val maxOfCostBPC:Long = Math.max(openCost, bytesPerCore)           // Larger of openCost and bytesPerCore
  val targetSize:Long =   Math.min(maxPartitionBytes , maxOfCostBPC) // Smaller of maxPartitionBytes and maxOfCostBPC
  val partions = paddedBytes.toDouble /  targetSize.toDouble         // The final number of partitions (needs to be rounded up)

  // Utility function to style each row
  def row(label:String, value:Long, extra:String=""):String = f"""<tr><td>$label:</td><td style="text-align:right; font-weight:bold">$value%,d</td><td style="padding-left:1em">$extra</td></tr>"""
  
  displayHTML("<table>" +
    row("File Count", files.size) +
    row("Actual Bytes", actualBytes) +
    row("Padded Bytes", paddedBytes, "Actual_Bytes + (File_Count * Open_Cost)") +
    row("Average Size", (paddedBytes/files.size).toLong) +
    """<tr><td colspan="2" style="border-top:1px solid black">&nbsp;</td></tr>""" +

    row("Open Cost", openCost, "spark.sql.files.openCostInBytes") +
    row("Bytes-Per-Core", bytesPerCore) +
    row("Max Cost", maxOfCostBPC, "(max of Open_Cost &amp; Bytes-Per-Core)") +
    """<tr><td colspan="2" style="border-top:1px solid black">&nbsp;</td></tr>""" +

    row("Max Partition Bytes", maxPartitionBytes, "spark.sql.files.maxPartitionBytes") +
    row("Target Size", targetSize, "(min of Max_Cost &amp; Max_Partition_Bytes)") +
    """<tr><td colspan="2" style="border-top:1px solid black">&nbsp;</td></tr>""" +
    
    row("Number of Partions", Math.ceil(partions).toLong, f"($partions from Padded_Bytes / Target_Size)") +
    "</table>")
}

// COMMAND ----------

// MAGIC %md # Test with different openCostInBytes

// COMMAND ----------

sc.setJobDescription("Step B: List Files")

// Source directory for this experiment's dataset
val tinyPath = "dbfs:/mnt/training/global-sales/transactions/2013.parquet"

// All the parquet files in this dataset
val tinyFiles = dbutils.fs.ls(tinyPath).filter(_.name.endsWith(".parquet"))

// Providing the schema precludes side effects that would otherwise skew benchmarks
val tinySchema = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,18), city_id integer"

display(tinyFiles)

// COMMAND ----------

sc.setJobDescription("Step C: OCB 4 mb")

spark.conf.set("spark.sql.files.openCostInBytes", 4194304) // Use the default 4MB

predictNumPartitions(tinyFiles)                            // Print our predictions

spark.read.schema(tinySchema).parquet(tinyPath)            // Load the transactions table
     .write.format("noop").mode("overwrite").save()        // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step D: OCB 1/2 MB")

spark.conf.set("spark.sql.files.openCostInBytes", 524288) // Reduce to 1/2 MB

predictNumPartitions(tinyFiles)                           // Print our predictions

spark.read.schema(tinySchema).parquet(tinyPath)           // Load the transactions table
     .write.format("noop").mode("overwrite").save()       // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step E: OCB 1/8 MB")

spark.conf.set("spark.sql.files.openCostInBytes", 131072) // Reduce to 1/8 MB

predictNumPartitions(tinyFiles)                           // Print our predictions

spark.read.schema(tinySchema).parquet(tinyPath)           // Load the transactions table
     .write.format("noop").mode("overwrite").save()       // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step F: OCB 0 MB")

spark.conf.set("spark.sql.files.openCostInBytes", 0)      // Reduce to 0 MB

predictNumPartitions(tinyFiles)                           // Print our predictions

spark.read.schema(tinySchema).parquet(tinyPath)           // Load the transactions table
     .write.format("noop").mode("overwrite").save()       // Test with a noop write