// Databricks notebook source
// MAGIC %scala
// MAGIC sc.setJobDescription("Step A: Basic initialization")
// MAGIC 
// MAGIC // Disable the Delta IO Cache to avoid side effects
// MAGIC spark.conf.set("spark.databricks.io.cache.enabled", false)

// COMMAND ----------

// MAGIC %scala
// MAGIC sc.setJobDescription("Step B: Create Table")
// MAGIC 
// MAGIC val initDF = spark
// MAGIC   .read
// MAGIC   .format("delta")
// MAGIC   .load("dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-par_year.delta")
// MAGIC 
// MAGIC initDF.createOrReplaceTempView("transactions")
// MAGIC 
// MAGIC // Printing the schema here forces spark to read the schema,
// MAGIC // something the Scala version will do, but not Python version, 
// MAGIC // because they share the same SparkSession (e.g. this notebook).
// MAGIC // Otherwise, our initial benchmarks would be off.
// MAGIC initDF.printSchema

// COMMAND ----------

// MAGIC %scala
// MAGIC sc.setJobDescription("Step C: Scala Baseline")
// MAGIC 
// MAGIC val baseTrxDF = spark
// MAGIC   .read.table("transactions")
// MAGIC   .filter("p_transacted_year = 2011")
// MAGIC   .select("description")
// MAGIC 
// MAGIC baseTrxDF.write.mode("overwrite").format("noop").save()

// COMMAND ----------

// MAGIC %scala
// MAGIC sc.setJobDescription("Step D: Scala higher-order functions")
// MAGIC 
// MAGIC import org.apache.spark.sql.functions._
// MAGIC 
// MAGIC val trxDF = baseTrxDF
// MAGIC   .withColumn("ccd_id", regexp_extract($"description", "ccd id: \\d+", 0))
// MAGIC   .withColumn("ppd_id", regexp_extract($"description", "ppd id: \\d+", 0))
// MAGIC   .withColumn("arc_id", regexp_extract($"description", "arc id: \\d+", 0))
// MAGIC   .withColumn("temp_id", when($"ccd_id" =!= "", $"ccd_id")
// MAGIC                         .when($"ppd_id" =!= "", $"ppd_id")
// MAGIC                         .when($"arc_id" =!= "", $"arc_id")
// MAGIC                         .otherwise(null))
// MAGIC   .withColumn("trxType", regexp_replace(split($"temp_id", ": ")(0), " id", ""))
// MAGIC   .withColumn("id", split($"temp_id", ": ")(1))
// MAGIC   .drop("ccd_id", "ppd_id", "arc_id", "temp_id")
// MAGIC 
// MAGIC trxDF.write.mode("overwrite").format("noop").save()

// COMMAND ----------

// MAGIC %scala
// MAGIC sc.setJobDescription("Step E: Scala UDF")
// MAGIC 
// MAGIC def parserId(description:String): String = {
// MAGIC   val ccdId = "ccd id: \\d+".r.findFirstIn(description).getOrElse(null)
// MAGIC   if (ccdId != null) return ccdId.substring(8)
// MAGIC   
// MAGIC   val ppdId = "ppd id: \\d+".r.findFirstIn(description).getOrElse(null)
// MAGIC   if (ppdId != null) return ppdId.substring(8)
// MAGIC   
// MAGIC   val arcId = "arc id: \\d+".r.findFirstIn(description).getOrElse(null)
// MAGIC   if (arcId != null) return arcId.substring(8)
// MAGIC   
// MAGIC   return null
// MAGIC }
// MAGIC def parseType(description:String): String = {
// MAGIC   val ccdId = "ccd id: \\d+".r.findFirstIn(description).getOrElse(null)
// MAGIC   if (ccdId != null) return ccdId.substring(0, 3)
// MAGIC   
// MAGIC   val ppdId = "ppd id: \\d+".r.findFirstIn(description).getOrElse(null)
// MAGIC   if (ppdId != null) return ppdId.substring(0, 3)
// MAGIC   
// MAGIC   val arcId = "arc id: \\d+".r.findFirstIn(description).getOrElse(null)
// MAGIC   if (arcId != null) return arcId.substring(0, 3)
// MAGIC   
// MAGIC   return null
// MAGIC }
// MAGIC 
// MAGIC val parserIdUDF = spark.udf.register("parserId", parserId _)
// MAGIC val parseTypeUDF = spark.udf.register("parseType", parseType _)
// MAGIC 
// MAGIC val trxDF = baseTrxDF
// MAGIC   .withColumn("trxType", parseTypeUDF($"description"))
// MAGIC   .withColumn("id", parserIdUDF($"description"))
// MAGIC 
// MAGIC trxDF.write.mode("overwrite").format("noop").save()

// COMMAND ----------

// MAGIC %scala
// MAGIC sc.setJobDescription("Step F: Typed Transformation")
// MAGIC 
// MAGIC case class Before(description:String)
// MAGIC case class After(description:String, trxType:String, id:String)
// MAGIC 
// MAGIC val trxDF = baseTrxDF
// MAGIC   .as[Before]
// MAGIC   .map(before => {
// MAGIC     val description = before.description
// MAGIC     val ccdId = "ccd id: \\d+".r.findFirstIn(description).getOrElse(null)
// MAGIC     val ppdId = "ppd id: \\d+".r.findFirstIn(description).getOrElse(null)
// MAGIC     val arcId = "arc id: \\d+".r.findFirstIn(description).getOrElse(null)
// MAGIC 
// MAGIC     if (ccdId != null)      After(description, ccdId.substring(0, 3), ccdId.substring(8))
// MAGIC     else if (ppdId != null) After(description, ppdId.substring(0, 3), ppdId.substring(8))
// MAGIC     else if (arcId != null) After(description, arcId.substring(0, 3), arcId.substring(8))
// MAGIC     else After(description, null, null)
// MAGIC   })
// MAGIC trxDF.write.mode("overwrite").format("noop").save()

// COMMAND ----------

// MAGIC %python
// MAGIC sc.setJobDescription("Step G: Python Baseline")
// MAGIC 
// MAGIC baseTrxDF = (spark
// MAGIC   .read.table("transactions")
// MAGIC   .filter("p_transacted_year = 2011")
// MAGIC   .select("description")
// MAGIC ) 
// MAGIC baseTrxDF.write.mode("overwrite").format("noop").save()

// COMMAND ----------

// MAGIC %python
// MAGIC sc.setJobDescription("Step H: Python higher-order functions")
// MAGIC 
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC trxDF = (baseTrxDF
// MAGIC   .withColumn("ccd_id", regexp_extract("description", "ccd id: \\d+", 0))
// MAGIC   .withColumn("ppd_id", regexp_extract("description", "ppd id: \\d+", 0))
// MAGIC   .withColumn("arc_id", regexp_extract("description", "arc id: \\d+", 0))
// MAGIC   .withColumn("temp_id", when(col("ccd_id") != "", col("ccd_id"))
// MAGIC                         .when(col("ppd_id") != "", col("ppd_id"))
// MAGIC                         .when(col("arc_id") != "", col("arc_id"))
// MAGIC                         .otherwise(None))
// MAGIC   .withColumn("trxType", regexp_replace(split("temp_id", ": ")[0], " id", ""))
// MAGIC   .withColumn("id", split("temp_id", ": ")[1])
// MAGIC   .drop("ccd_id", "ppd_id", "arc_id", "temp_id")
// MAGIC )
// MAGIC trxDF.write.mode("overwrite").format("noop").save()

// COMMAND ----------

// MAGIC %python
// MAGIC sc.setJobDescription("Step I: Python UDF")
// MAGIC 
// MAGIC import re
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC @udf('string')
// MAGIC def parserId(description):
// MAGIC   ccdId = re.findall("ccd id: \\d+", description)
// MAGIC   if len(ccdId) > 0: return ccdId[0][8:]
// MAGIC   
// MAGIC   ppdId =  re.findall("ppd id: \\d+", description)
// MAGIC   if len(ppdId) > 0: return ppdId[0][8:]
// MAGIC   
// MAGIC   arcId =  re.findall("arc id: \\d+", description)
// MAGIC   if len(arcId) > 0: return arcId[0][8:]
// MAGIC   
// MAGIC   return None
// MAGIC 
// MAGIC @udf('string')
// MAGIC def parseType(description):
// MAGIC   ccdId = re.findall("ccd id: \\d+", description)
// MAGIC   if len(ccdId) > 0: return ccdId[0][0:3]
// MAGIC   
// MAGIC   ppdId =  re.findall("ppd id: \\d+", description)
// MAGIC   if len(ppdId) > 0: return ppdId[0][0:3]
// MAGIC   
// MAGIC   arcId =  re.findall("arc id: \\d+", description)
// MAGIC   if len(arcId) > 0: return arcId[0][0:3]
// MAGIC   
// MAGIC   return None
// MAGIC 
// MAGIC trxDF = (baseTrxDF
// MAGIC   .withColumn("trxType", parseType("description"))
// MAGIC   .withColumn("id", parserId("description"))
// MAGIC )
// MAGIC trxDF.write.mode("overwrite").format("noop").save()

// COMMAND ----------

// MAGIC %python
// MAGIC sc.setJobDescription("Step J: Python Vectorized UDFs")
// MAGIC 
// MAGIC import re
// MAGIC import pandas as pd
// MAGIC from pyspark.sql.functions import *
// MAGIC from pyspark.sql.types import StringType
// MAGIC 
// MAGIC @pandas_udf('string')
// MAGIC def parserId(descriptions: pd.Series) -> pd.Series:
// MAGIC   def _parse_id(description):
// MAGIC     ccdId = re.findall("ccd id: \\d+", description)
// MAGIC     if len(ccdId) > 0: return ccdId[0][8:]
// MAGIC     
// MAGIC     ppdId =  re.findall("ppd id: \\d+", description)
// MAGIC     if len(ppdId) > 0: return ppdId[0][8:]
// MAGIC     
// MAGIC     arcId =  re.findall("arc id: \\d+", description)
// MAGIC     if len(arcId) > 0: return arcId[0][8:]
// MAGIC     
// MAGIC     return None
// MAGIC   
// MAGIC   return descriptions.map(_parse_id)
// MAGIC 
// MAGIC @pandas_udf('string')
// MAGIC def parseType(descriptions: pd.Series) -> pd.Series:
// MAGIC   def _parse_type(description):
// MAGIC     ccdId = re.findall("ccd id: \\d+", description)
// MAGIC     if len(ccdId) > 0: return ccdId[0][0:3]
// MAGIC     
// MAGIC     ppdId =  re.findall("ppd id: \\d+", description)
// MAGIC     if len(ppdId) > 0: return ppdId[0][0:3]
// MAGIC     
// MAGIC     arcId =  re.findall("arc id: \\d+", description)
// MAGIC     if len(arcId) > 0: return arcId[0][0:3]
// MAGIC     
// MAGIC     return None
// MAGIC 
// MAGIC   return descriptions.map(_parse_type)
// MAGIC 
// MAGIC trxDF = (baseTrxDF
// MAGIC   .withColumn("trxType", parseType("description"))
// MAGIC   .withColumn("id", parserId("description"))
// MAGIC )
// MAGIC trxDF.write.mode("overwrite").format("noop").save()