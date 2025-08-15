# Databricks notebook source
sc.setJobDescription("Step A: Basic initialization")

# Disable the Delta IO Cache to avoid side effects
spark.conf.set("spark.databricks.io.cache.enabled", False)

# COMMAND ----------

sc.setJobDescription("Step B: Create Table")

initDF = (spark
  .read
  .format("delta")
  .load("dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-par_year.delta")
)
initDF.createOrReplaceTempView("transactions")

# Printing the schema here forces spark to read the schema
# avoiding side effects in future benchmarks
initDF.printSchema()

# COMMAND ----------

sc.setJobDescription("Step C: Establish a baseline")

baseTrxDF = (spark
  .read.table("transactions")
  .select("description")
) 
baseTrxDF.write.mode("overwrite").format("noop").save()

# COMMAND ----------

sc.setJobDescription("Step D: Higher-order functions")

from pyspark.sql.functions import *

trxDF = (baseTrxDF
  .withColumn("ccd_id", regexp_extract("description", "ccd id: \\d+", 0))
  .withColumn("ppd_id", regexp_extract("description", "ppd id: \\d+", 0))
  .withColumn("arc_id", regexp_extract("description", "arc id: \\d+", 0))
  .withColumn("temp_id", when(col("ccd_id") != "", col("ccd_id"))
                        .when(col("ppd_id") != "", col("ppd_id"))
                        .when(col("arc_id") != "", col("arc_id"))
                        .otherwise(None))
  .withColumn("trxType", regexp_replace(split("temp_id", ": ")[0], " id", ""))
  .withColumn("id", split("temp_id", ": ")[1])
  .drop("ccd_id", "ppd_id", "arc_id", "temp_id")
)
trxDF.write.mode("overwrite").format("noop").save()

# COMMAND ----------

sc.setJobDescription("Step E: UDFs")

import re
from pyspark.sql.functions import *

@udf('string')
def parserId(description):
  ccdId = re.findall("ccd id: \\d+", description)
  if len(ccdId) > 0: return ccdId[0][8:]
  
  ppdId =  re.findall("ppd id: \\d+", description)
  if len(ppdId) > 0: return ppdId[0][8:]
  
  arcId =  re.findall("arc id: \\d+", description)
  if len(arcId) > 0: return arcId[0][8:]
  
  return None

@udf('string')
def parseType(description):
  ccdId = re.findall("ccd id: \\d+", description)
  if len(ccdId) > 0: return ccdId[0][0:3]
  
  ppdId =  re.findall("ppd id: \\d+", description)
  if len(ppdId) > 0: return ppdId[0][0:3]
  
  arcId =  re.findall("arc id: \\d+", description)
  if len(arcId) > 0: return arcId[0][0:3]
  
  return None

trxDF = (baseTrxDF
  .withColumn("trxType", parseType("description"))
  .withColumn("id", parserId("description"))
)
trxDF.write.mode("overwrite").format("noop").save()

# COMMAND ----------

sc.setJobDescription("Step F: Python Vectorized UDFs")

import re
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

@pandas_udf('string')
def parserId(descriptions: pd.Series) -> pd.Series:
  def _parse_id(description):
    ccdId = re.findall("ccd id: \\d+", description)
    if len(ccdId) > 0: return ccdId[0][8:]
    
    ppdId =  re.findall("ppd id: \\d+", description)
    if len(ppdId) > 0: return ppdId[0][8:]
    
    arcId =  re.findall("arc id: \\d+", description)
    if len(arcId) > 0: return arcId[0][8:]
    
    return None
  
  return descriptions.map(_parse_id)

@pandas_udf('string')
def parseType(descriptions: pd.Series) -> pd.Series:
  def _parse_type(description):
    ccdId = re.findall("ccd id: \\d+", description)
    if len(ccdId) > 0: return ccdId[0][0:3]
    
    ppdId =  re.findall("ppd id: \\d+", description)
    if len(ppdId) > 0: return ppdId[0][0:3]
    
    arcId =  re.findall("arc id: \\d+", description)
    if len(arcId) > 0: return arcId[0][0:3]
    
    return None

  return descriptions.map(_parse_type)

trxDF = (baseTrxDF
  .withColumn("trxType", parseType("description"))
  .withColumn("id", parserId("description"))
)
trxDF.write.mode("overwrite").format("noop").save()