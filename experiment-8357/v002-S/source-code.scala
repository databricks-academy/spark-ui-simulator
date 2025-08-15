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

sc.setJobDescription("Step A: Basic initialization")

import org.apache.spark.sql.functions._

// Configure this app to connect to a PostgreSQL database
Class.forName("org.postgresql.Driver")

val connectionProperties = new java.util.Properties()
connectionProperties.put("user", "readonly")
connectionProperties.put("password", "readonly")

val tableName = "training.people_1m"
val jdbcUrl = "jdbc:postgresql://server1.databricks.training:5432/training"

// COMMAND ----------

sc.setJobDescription("Step B: Default Read")

val defaultReadDF = spark
  .read.jdbc(                                               // Open a JDBC connection
    jdbcUrl,                                                // The JDBC URL
    tableName,                                              // The name of the table
    connectionProperties)                                   // The connection properties

defaultReadDF.write.format("noop").mode("overwrite").save() // Execute a noop write to test

val partions = defaultReadDF.rdd.getNumPartitions           // Ask the RDD, how many partitions?

// COMMAND ----------

sc.setJobDescription("Step C: Known Stride")

val knownReadDF = spark
  .read.jdbc(                                           // Open a JDBC connection
    jdbcUrl,                                            // The JDBC URL
    tableName,                                          // The name of the table
    "id",                                               // The name of a column of an integral type that will be used for partitioning.
    1,                                                  // The minimum value of columnName used to decide partition stride.
    1000000,                                            // The maximum value of columnName used to decide partition stride
    16,                                                 // The number of partitions
    connectionProperties)                               // The connection properties

knownReadDF.write.format("noop").mode("overwrite").save() // Execute a noop write to test

val partions = knownReadDF.rdd.getNumPartitions           // Ask the RDD, how many partitions?

// COMMAND ----------

sc.setJobDescription("Step D-1: Unknown Stride, range test")

val minimumID = defaultReadDF // Using the "default" connection parameters
  .select(min($"id"))         // Compute the minimum ID
  .first().getInt(0)          // Extract as an integer

val maximumID = defaultReadDF // Using the "default" connection parameters
  .select(max($"id"))         // Compute the maximum ID
  .first().getInt(0)          // Extract as an integer

// COMMAND ----------

sc.setJobDescription("Step D-2: Unknown Stride, actual query")

val unknownReadDF = spark
  .read.jdbc(                                               // Open a JDBC connection
    jdbcUrl,                                                // The JDBC URL
    tableName,                                              // The name of the table
    "id",                                                   // The name of a column of an integral type that will be used for partitioning.
    minimumID,                                              // The minimum value of columnName used to decide partition stride.
    maximumID,                                              // The maximum value of columnName used to decide partition stride
    sc.defaultParallelism,                                  // IMPORTANT: Max cores must match the database's safe (and/or maximum) concurrent connections
    connectionProperties)                                   // The connection properties

unknownReadDF.write.format("noop").mode("overwrite").save() // Execute a noop write to test

val partions = unknownReadDF.rdd.getNumPartitions           // Ask the RDD, how many partitions?