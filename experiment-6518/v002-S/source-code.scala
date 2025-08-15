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

import org.apache.spark.sql.functions._

// Disable IO cache so as to minimize side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

// Disable all Spark 3 features
spark.conf.set("spark.sql.adaptive.enabled", false)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", false)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

// COMMAND ----------

sc.setJobDescription("Step A-2: Register our Spill Listener")
// Stolen the Apache Spark test suite, TestUtils
// https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/TestUtils.scala

class SpillListener extends org.apache.spark.scheduler.SparkListener {
  import org.apache.spark.scheduler.{SparkListenerTaskEnd,SparkListenerStageCompleted}
  import org.apache.spark.executor.TaskMetrics
  import scala.collection.mutable.{HashMap,ArrayBuffer}

  private val stageIdToTaskMetrics = new HashMap[Int, ArrayBuffer[TaskMetrics]]
  private val spilledStageIds = new scala.collection.mutable.HashSet[Int]

  def numSpilledStages: Int = synchronized {spilledStageIds.size}
  def reset(): Unit = synchronized { spilledStageIds.clear }
  def report(): Unit = synchronized { println(f"Spilled Stages: ${numSpilledStages}%,d") }
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {stageIdToTaskMetrics.getOrElseUpdate(taskEnd.stageId, new ArrayBuffer[TaskMetrics]) += taskEnd.taskMetrics}

  override def onStageCompleted(stageComplete: SparkListenerStageCompleted): Unit = synchronized {
    val stageId = stageComplete.stageInfo.stageId
    val metrics = stageIdToTaskMetrics.remove(stageId).toSeq.flatten
    val spilled = metrics.map(_.memoryBytesSpilled).sum > 0
    if (spilled) spilledStageIds += stageId
  }
}
val spillListener = new SpillListener()
sc.addSparkListener(spillListener)

// COMMAND ----------

sc.setJobDescription("Step B: shuffle.partitions")
spillListener.reset()

// Create a large partition by mismanaging shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 60)

// The location of our non-skewed set of transactions
val trxPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018_int-only_60gb.delta"

spark
  .read.format("delta").load(trxPath)            // Load the transactions table
  .orderBy("trx_id")                             // Some wide transformation
  .write.format("noop").mode("overwrite").save() // Execute a noop write to test

spillListener.report()

// COMMAND ----------

sc.setJobDescription("Step C: union")
spillListener.reset()

// We start with 480 partitions, finish with 480
spark.conf.set("spark.sql.shuffle.partitions", 480)

// The location of our non-skewed set of transactions
val trxPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018_int-only_60gb.delta"

// Load the table and union to itself 8 times
var trxDF = spark.read.format("delta").load(trxPath)  // Load the transactions table
     .union(spark.read.format("delta").load(trxPath)) // Union it to itself..
     .union(spark.read.format("delta").load(trxPath)) // ...again
     .union(spark.read.format("delta").load(trxPath)) // ...again
     .union(spark.read.format("delta").load(trxPath)) // ...
     .union(spark.read.format("delta").load(trxPath)) // ...
     .union(spark.read.format("delta").load(trxPath)) // ...
     .union(spark.read.format("delta").load(trxPath)) // ... and again
     .orderBy("trx_id")                               // Some wide transformation
     .write.format("noop").mode("overwrite").save()   // Execute a noop write to test

spillListener.report()

// COMMAND ----------

sc.setJobDescription("Step D: explode")
spillListener.reset()

// We start with 480 partitions, finish with 480
spark.conf.set("spark.sql.shuffle.partitions", 480)

// The location of our non-skewed set of transactions
val trxPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018_int-only_60gb.delta"

// An array that will grow each partions by a factor of 8
val data = Seq(0,1,3,4,5,6,7,8).toArray

val count = spark
  .read.format("delta").load(trxPath)            // Load the transactions table
  .withColumn("stuff", lit(data))                // Add an array of N items
  .select($"*", explode($"stuff"))               // Explode our dataset in size by N-1 items
  .distinct()                                    // Some wide transformation
  .write.format("noop").mode("overwrite").save() // Execute a noop write to test

spillListener.report()

// COMMAND ----------

sc.setJobDescription("Step E-1: Show the skew")

// The path to our cities table & skewed set of transactions
val ctyPath = "dbfs:/mnt/training/global-sales/cities/all.delta"
val trxPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.delta"

val resultsDF = spark
  .read.format("delta").load(trxPath) // Load the transactions table
  .groupBy("city_id").count           // Group by city_id and count
  .orderBy($"count")                  // Sort by count

display(resultsDF)

// COMMAND ----------

sc.setJobDescription("Step E-2: Skewed Join")
spillListener.reset()

// We start with 825 partitions, finish with 825
spark.conf.set("spark.sql.shuffle.partitions", 825)

val citiesDF = spark.read.format("delta").load(ctyPath)       // Load the city table
val transactionsDF = spark.read.format("delta").load(trxPath) // Load the transactions table

transactionsDF
  .join(citiesDF, "city_id")                                  // Join by city_id
  .write.format("noop").mode("overwrite").save()              // Execute a noop write to test

spillListener.report()