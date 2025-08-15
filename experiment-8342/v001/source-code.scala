// Databricks notebook source
import org.apache.spark.sql.functions._

Class.forName("org.postgresql.Driver")

val connectionProperties = new java.util.Properties()
connectionProperties.put("user", "readonly")
connectionProperties.put("password", "readonly")

val tableName = "training.people_1m"
val jdbcUrl = "jdbc:postgresql://server1.databricks.training:5432/training"

// COMMAND ----------

// JDBC without filter
spark.read
  .jdbc(url=jdbcUrl, table=tableName, properties=connectionProperties)
  .foreach(_=> ())

// COMMAND ----------

// JDBC with filter
spark.read
  .jdbc(url=jdbcUrl, table=tableName, properties=connectionProperties)
  .filter($"id" === 343517)
  .foreach(_=> ())

// COMMAND ----------

// JDBC with cache & filter
spark.read
  .jdbc(url=jdbcUrl, table=tableName, properties=connectionProperties)
  .cache()
  .filter($"id" === 343517)
  .foreach(_=> ())

// COMMAND ----------


