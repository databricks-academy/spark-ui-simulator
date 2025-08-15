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

// MAGIC %md # Test with different maxPartitionBytes

// COMMAND ----------

sc.setJobDescription("Step B: List Files")

// Source directory for this experiment's dataset
val trxPath = s"dbfs:/mnt/training/global-sales/solutions/1990-to-2009.parquet" 

// Providing the schema precludes side effects that would otherwise skew benchmarks
val trxSchema = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer, new_at timestamp"

// All the parquet files in this dataset
val trxFiles = dbutils.fs.ls(trxPath).filter(_.name.endsWith(".parquet"))

display(trxFiles)

// COMMAND ----------

sc.setJobDescription("Step C: Read at 1x")

val maxPartitionBytesConf = f"${defaultMaxPartitionBytes * 1}b"
spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytesConf)

predictNumPartitions(trxFiles)

spark.read.schema(trxSchema).parquet(trxPath)       // Load the transactions table
     .write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step D: Read at 2x")

val maxPartitionBytesConf = f"${defaultMaxPartitionBytes * 2}b"
spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytesConf)

predictNumPartitions(trxFiles)

spark.read.schema(trxSchema).parquet(trxPath)       // Load the transactions table
     .write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step E: Read at 4x")

val maxPartitionBytesConf = f"${defaultMaxPartitionBytes * 4}b"
spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytesConf)

predictNumPartitions(trxFiles)

spark.read.schema(trxSchema).parquet(trxPath)       // Load the transactions table
     .write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step F: Read at 8x")

val maxPartitionBytesConf = f"${defaultMaxPartitionBytes * 8}b" // ~1 GB
spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytesConf)

predictNumPartitions(trxFiles)

spark.read.schema(trxSchema).parquet(trxPath)       // Load the transactions table
     .write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step G: Read at 16x")

val maxPartitionBytesConf = f"${defaultMaxPartitionBytes * 16}b" // ~2 GB
spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytesConf)

predictNumPartitions(trxFiles)

spark.read.schema(trxSchema).parquet(trxPath)       // Load the transactions table
     .write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step H: Read at 32x")

val maxPartitionBytesConf = f"${defaultMaxPartitionBytes * 32}b" // ~4 GB
spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytesConf)

predictNumPartitions(trxFiles)

spark.read.schema(trxSchema).parquet(trxPath)       // Load the transactions table
     .write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

sc.setJobDescription("Step I: Read at 64x")

val maxPartitionBytesConf = f"${defaultMaxPartitionBytes * 64}b" // ~8 GB
spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytesConf)

predictNumPartitions(trxFiles)

spark.read.schema(trxSchema).parquet(trxPath)       // Load the transactions table
     .write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

// MAGIC %md # One Partition Per File

// COMMAND ----------

sc.setJobDescription("Step J-1: List One-To-One Files")

// Source directory for this experiment's dataset
val otoPath = "dbfs:/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet"

// The list of files and schema for this dataset
val otoFiles = dbutils.fs.ls(otoPath).filter(_.name.endsWith(".parquet"))

// Providing the schema precludes side effects that would otherwise skew benchmarks
val otoSchema = "timestamp string, site string, requests integer"

display(otoFiles)

// COMMAND ----------

sc.setJobDescription("Step J-2: Read One-To-One at 1x")

val maxPartitionBytesConf = f"${defaultMaxPartitionBytes * 1}b" // ~128 MB
spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytesConf)

predictNumPartitions(otoFiles)

spark.read.schema(otoSchema).parquet(otoPath)         // Load the transactions table
     .write.format("noop").mode("overwrite").save()   // Test with a noop write

// COMMAND ----------

// MAGIC %md
// MAGIC # Multiple Files to One Partition

// COMMAND ----------

sc.setJobDescription("Step K-1: List Many-To-One Files")

// Source directory for this experiment's dataset
val mtoPath = "dbfs:/mnt/training/crime-data-2016/Crime-Data-Philadelphia-2016.parquet"

// All the parquet files in this dataset
val mtoFiles = dbutils.fs.ls(mtoPath).filter(_.name.endsWith(".parquet"))

// Providing the schema precludes side effects that would otherwise skew benchmarks
val mtoSchema = "district integer, dispatch_date_time timestamp, dispatch_date timestamp, dispatch_time string, hour integer, unique_id long, location_block string, ucr_general integer, text_general_code string, point_x double, point_y double, lat double, lng double, ucr_general_description string"

display(mtoFiles)

// COMMAND ----------

sc.setJobDescription("Step K-2: Read Many-To-One at 1x")

val maxPartitionBytesConf = f"${defaultMaxPartitionBytes * 1}b" // ~128 MB
spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytesConf)

predictNumPartitions(mtoFiles)

spark.read.schema(mtoSchema).parquet(mtoPath)       // Load the transactions table
     .write.format("noop").mode("overwrite").save() // Test with a noop write

// COMMAND ----------

// MAGIC %md
// MAGIC # Auto-tune maxPartitionBytes

// COMMAND ----------

sc.setJobDescription("Step L-1: Autotune maxPartitionBytes Function")

def autoTuneMaxPartitionBytes(format:String, path:String, schema:String, maxSteps:Int, startingBytes:Long=134217728):Long = {
  
  var cores = sc.defaultParallelism
  var maxPartitionBytes:Long = startingBytes
  val originalMaxPartitionBytes = spark.conf.get("spark.sql.files.maxPartitionBytes")
  
  for (step <- 0 to maxSteps) {
    maxPartitionBytes = maxPartitionBytes + (step * 1024 * 1024)
    val maxPartitionMB = maxPartitionBytes / 1024 / 1024
    
    spark.conf.set("spark.sql.files.maxPartitionBytes", f"${maxPartitionBytes}b")

    val partitions = spark.read.format(format).schema(schema).load(path).rdd.getNumPartitions

    if (partitions % cores == 0) {
      println("*** Found it! ***")
      println(f"$maxPartitionMB%,d MB with $partitions%,d partitions, iterations: ${partitions/cores.toDouble}")
      return maxPartitionBytes
      
    } else {
      println(f"$maxPartitionMB%,d MB with $partitions%,d partitions, iterations: ${partitions/cores.toDouble}")
    }
  }
  spark.conf.set("spark.sql.files.maxPartitionBytes", originalMaxPartitionBytes)
  throw new IllegalArgumentException("An appropriate maxPartitionBytes was not found")
}

// COMMAND ----------

sc.setJobDescription("Step L-2: Autotuned maxPartitionBytes")

val maxPartitionBytes = autoTuneMaxPartitionBytes("parquet", trxPath, trxSchema, 100)

println("-"*80)
println(f"Final Answer: $maxPartitionBytes%,d bytes or ${maxPartitionBytes/1024/1024}%,d MB")
println("-"*80)