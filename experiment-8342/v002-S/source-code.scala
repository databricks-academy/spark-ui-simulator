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

sc.setJobDescription("Step B: No Predicate")

spark.read
  .jdbc(url=jdbcUrl, table=tableName, properties=connectionProperties) // Open a JDBC connect
  .write.format("noop").mode("overwrite").save()                       // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step C: With Predicate")

spark.read
  .jdbc(url=jdbcUrl, table=tableName, properties=connectionProperties) // Open a JDBC connect
  .filter($"id" === 343517)                                            // One and only one record
  .write.format("noop").mode("overwrite").save()                       // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step D: Cripppled Predicate")

spark.read
  .jdbc(url=jdbcUrl, table=tableName, properties=connectionProperties) // Open a JDBC connect
  .cache()                                                             // An innocent mistake
  .filter($"id" === 343517)                                            // One and only one record
  .write.format("noop").mode("overwrite").save()                       // Execute a noop write to test