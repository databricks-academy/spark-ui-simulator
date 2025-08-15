// Databricks notebook source
sc.setJobDescription("Step A: Basic initialization")

// Disable the Delta IO Cache to avoid side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

// COMMAND ----------

sc.setJobDescription("Step B: Create Table")

val initDF = spark
  .read
  .format("delta")
  .load("dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-par_year.delta")

initDF.createOrReplaceTempView("transactions")

// Printing the schema here forces spark to read the schema
// avoiding side effects in future benchmarks
initDF.printSchema

// COMMAND ----------

sc.setJobDescription("Step C: Establish a baseline")

val baseTrxDF = spark
  .read.table("transactions")
  .select("description")

baseTrxDF.write.mode("overwrite").format("noop").save()

// COMMAND ----------

sc.setJobDescription("Step D: Higher-order functions")

import org.apache.spark.sql.functions._

val trxDF = baseTrxDF
  .withColumn("ccd_id", regexp_extract($"description", "ccd id: \\d+", 0))
  .withColumn("ppd_id", regexp_extract($"description", "ppd id: \\d+", 0))
  .withColumn("arc_id", regexp_extract($"description", "arc id: \\d+", 0))
  .withColumn("temp_id", when($"ccd_id" =!= "", $"ccd_id")
                        .when($"ppd_id" =!= "", $"ppd_id")
                        .when($"arc_id" =!= "", $"arc_id")
                        .otherwise(null))
  .withColumn("trxType", regexp_replace(split($"temp_id", ": ")(0), " id", ""))
  .withColumn("id", split($"temp_id", ": ")(1))
  .drop("ccd_id", "ppd_id", "arc_id", "temp_id")

trxDF.write.mode("overwrite").format("noop").save()

// COMMAND ----------

sc.setJobDescription("Step E: UDFs")

def parserId(description:String): String = {
  val ccdId = "ccd id: \\d+".r.findFirstIn(description).getOrElse(null)
  if (ccdId != null) return ccdId.substring(8)
  
  val ppdId = "ppd id: \\d+".r.findFirstIn(description).getOrElse(null)
  if (ppdId != null) return ppdId.substring(8)
  
  val arcId = "arc id: \\d+".r.findFirstIn(description).getOrElse(null)
  if (arcId != null) return arcId.substring(8)
  
  return null
}
def parseType(description:String): String = {
  val ccdId = "ccd id: \\d+".r.findFirstIn(description).getOrElse(null)
  if (ccdId != null) return ccdId.substring(0, 3)
  
  val ppdId = "ppd id: \\d+".r.findFirstIn(description).getOrElse(null)
  if (ppdId != null) return ppdId.substring(0, 3)
  
  val arcId = "arc id: \\d+".r.findFirstIn(description).getOrElse(null)
  if (arcId != null) return arcId.substring(0, 3)
  
  return null
}

val parserIdUDF = spark.udf.register("parserId", parserId _)
val parseTypeUDF = spark.udf.register("parseType", parseType _)

val trxDF = baseTrxDF
  .withColumn("trxType", parseTypeUDF($"description"))
  .withColumn("id", parserIdUDF($"description"))

trxDF.write.mode("overwrite").format("noop").save()

// COMMAND ----------

sc.setJobDescription("Step F: Typed Transformation")

case class Before(description:String)
case class After(description:String, trxType:String, id:String)

val trxDF = baseTrxDF
  .as[Before]
  .map(before => {
    val description = before.description
    val ccdId = "ccd id: \\d+".r.findFirstIn(description).getOrElse(null)
    val ppdId = "ppd id: \\d+".r.findFirstIn(description).getOrElse(null)
    val arcId = "arc id: \\d+".r.findFirstIn(description).getOrElse(null)

    if (ccdId != null)      After(description, ccdId.substring(0, 3), ccdId.substring(8))
    else if (ppdId != null) After(description, ppdId.substring(0, 3), ppdId.substring(8))
    else if (arcId != null) After(description, arcId.substring(0, 3), arcId.substring(8))
    else After(description, null, null)
  })
trxDF.write.mode("overwrite").format("noop").save()