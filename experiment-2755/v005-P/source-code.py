# Databricks notebook source
# MAGIC %md
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <td></td>
# MAGIC     <td>VM</td>
# MAGIC     <td>Quantity</td>
# MAGIC     <td>Total Cores</td>
# MAGIC     <td>Total RAM</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>Driver:</td>
# MAGIC     <td>**i3.2xlarge**</td>
# MAGIC     <td>**1**</td>
# MAGIC     <td>**8 cores**</td>
# MAGIC     <td>**61 GB**</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>Workers:</td>
# MAGIC     <td>**i3.2xlarge**</td>
# MAGIC     <td>**2**</td>
# MAGIC     <td>**16 cores**</td>
# MAGIC     <td>**122 GB**</td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

from pyspark.sql.functions import *

# Disable IO cache so as to minimize side effects
spark.conf.set("spark.databricks.io.cache.enabled", False)


# COMMAND ----------

# MAGIC %fs ls /mnt/training/global-sales/transactions/

# COMMAND ----------

spark.read.parquet("dbfs:/mnt/training/global-sales/transactions/2011.parquet/").count()

# COMMAND ----------

trxPath = "dbfs:/mnt/training/global-sales/transactions/2011.parquet"
resultsDF = (spark
  .read.format("parquet").load(trxPath) # Load the transactions table
  .groupBy("city_id").count()           # Group by city_id and count
  .orderBy("count"))                   # Sort by count

display(resultsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The column city_id is skewed. We could build our queries using that column and show the spill, but to demonstrate the concept more clearly, I am going to create a static column called "val", set it to a static value and create my query based on that. This will create a much more evident skew and could likely cause the cluster to fail.

# COMMAND ----------

spark.read.format("parquet").load("dbfs:/mnt/training/global-sales/transactions/2011.parquet").schema

# COMMAND ----------

import pandas as pd  
from pyspark.sql.functions import pandas_udf, ceil, lit
from hashlib import sha256

sc.setJobDescription("Step A: groupBy+applyInPandas unhandled skew")
df = spark.read.format("parquet").load("dbfs:/mnt/training/global-sales/transactions/2011.parquet").withColumn("val", lit(1))
#limiting the rows to 50% avoid OOM
limit_count = 16693997

def find_longest(pdf):
    v = ''
    for t in pdf.description:
        if len(t) > len(v):
            v=t
    return pdf.assign(description=v)

df.limit(limit_count).groupby("val").applyInPandas(
    find_longest, schema="city_id string, amount string, retailer_id string, trx_id string, transacted_at string, val long, description String").write.format("noop").mode("Overwrite").save() 

# COMMAND ----------

sc.setJobDescription("Step B: Window function skew on partitionBy column")
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# The location of our skewed set of transactions
trxPath = "dbfs:/mnt/training/global-sales/transactions/2011.parquet"

df = (spark
          .read
          .format("parquet")
          .load(trxPath)
          .withColumn("val", lit(1)))                     # Load the transactions table

window_spec = Window.partitionBy("val")
(df
     .withColumn("total_amount", sum(col("amount")).over(window_spec))
     .write
     .format("noop")
     .mode("overwrite")
     .save())                      # Execute a noop write to test


# COMMAND ----------

# MAGIC %md If we take away the limit, we will get an OOM error. 

# COMMAND ----------

import pandas as pd  
from pyspark.sql.functions import pandas_udf, ceil, lit
from hashlib import sha256

sc.setJobDescription("Step A: groupBy+applyInPandas unhandled skew")
df = spark.read.format("parquet").load("dbfs:/mnt/training/global-sales/transactions/2011.parquet").withColumn("val", lit(1))


def find_longest(pdf):
    v = ''
    for t in pdf.description:
        if len(t) > len(v):
            v=t
    return pdf.assign(description=v)

df.groupby("val").applyInPandas(
    find_longest, schema="city_id string, amount string, retailer_id string, trx_id string, transacted_at string, val long, description String").write.format("noop").mode("Overwrite").save() 