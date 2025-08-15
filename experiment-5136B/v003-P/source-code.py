# Databricks notebook source
# MAGIC %md
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <td></td>
# MAGIC     <td>VM</td>
# MAGIC     <td>Quantity</td>
# MAGIC     <td>Total Cores</td>
# MAGIC     <td>Total RAM</td>
# MAGIC     <td>IOPS/MBps</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>Driver:</td>
# MAGIC     <td>**Standard_L8s**</td>
# MAGIC     <td>**1**</td>
# MAGIC     <td>**8 cores**</td>
# MAGIC     <td>**64 GB**</td>
# MAGIC     <td></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>Workers:</td>
# MAGIC     <td>**Standard_L8s**</td>
# MAGIC     <td>**10**</td>
# MAGIC     <td>**80 cores**</td>
# MAGIC     <td>**640 GB**</td>
# MAGIC     <td>**40,000 / 400**</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC <!-- https://docs.microsoft.com/en-us/azure/virtual-machines/sizes-previous-gen#ls-series -->

# COMMAND ----------

# DBTITLE 1,Basic Initialization
sc.setJobDescription("Step A: Basic initialization")

# magic number that will produce 1 TB of data
record_count = 15500000000

# 1 GB per partition
partitions = 512

# COMMAND ----------

# DBTITLE 1,Force Shuffle & Spill
sc.setJobDescription("Step B: Shuffle w/Spill")
from pyspark.sql.functions import sha2, col

spark.conf.set("spark.sql.shuffle.partitions", partitions)

df_1 = (spark
  .range(0, record_count-1, 1, partitions)
  .withColumnRenamed("id", "long_value_1")
  .withColumn("hash_value_1", sha2(col("long_value_1").cast("string"), 256))
)

df_2 = (spark
  .range(0, record_count, 1, partitions)
  .withColumnRenamed("id", "long_value_2")
  .withColumn("hash_value_2", sha2(col("long_value_2").cast("string"), 256))
)

df = df_1.join(df_2, col("long_value_1") == col("long_value_2"))
df.write.format("noop").mode("overwrite").save()