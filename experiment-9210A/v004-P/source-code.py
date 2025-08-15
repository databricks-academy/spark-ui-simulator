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
# MAGIC     <td>**4**</td>
# MAGIC     <td>**8 cores**</td>
# MAGIC     <td>**61 GB**</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC 
# MAGIC Databricks Runtime: <b>10.4</b>

# COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")

from pyspark.sql.functions import *

# Disable the Parquet/Delta IO Cache to avoid side effects
spark.conf.set("spark.databricks.io.cache.enabled", "false")

# Setting shuffle partitions to a high number where only ~800 are really needed
spark.conf.set("spark.sql.shuffle.partitions", 3000)

# Source paths for our wide and lookup dataset
source_base_path = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/experiments/exp-9210"
wide_source_path = f"{source_base_path}/wide_facts.delta"
lookup_source_path = f"{source_base_path}/lookup_dim.delta"

# Target paths for our wide and joined dataset
target_base_path = "dbfs:/dbacademy/experiments/exp-9210A"
dbutils.fs.rm(target_base_path, True) # just in case

wide_target_path = f"{target_base_path}/wide_target.delta"
joined_target_path = f"{target_base_path}/joined_target.delta"
joined_target_path2 = f"{target_base_path}/joined_target2.delta"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading & Writing Wide Tables wo/Photon

# COMMAND ----------

wide_df = spark.read.format("delta").load(wide_source_path)

# COMMAND ----------

sc.setJobDescription("Step D: Read 200-column Delta table with No-Op Write")

wide_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

sc.setJobDescription("Step E: Read & write 200-column Delta table")

wide_df.write.format("delta").mode("overwrite").save(wide_target_path)

# COMMAND ----------

# MAGIC %md ## Joining Wide Tables wo/Photon

# COMMAND ----------

lookup_df = spark.read.format("delta").load(lookup_source_path) # Load the lookup table

wide_df = (spark.read.format("delta").load(wide_source_path))   # Load the wide table

results_df = (wide_df
  .join(lookup_df, wide_df["lookup_id"] == lookup_df["id"])     # Join by lookup_id
  .drop(lookup_df["id"])
)

# COMMAND ----------

sc.setJobDescription("Step F: Wide Table Join with No-Op Write")

results_df.write.format("noop").mode("overwrite").save()                

# COMMAND ----------

sc.setJobDescription("Step G: Write Wide Table Join")

results_df.write.format("delta").mode("overwrite").save(joined_target_path)

# COMMAND ----------

# MAGIC %md ##Adding some derived columns

# COMMAND ----------

results_df = (results_df
                  .withColumn("derived1", concat(col("lookup_id"), col("column_3"), col("column_6")))
                  .withColumn("derived2", concat(col("column_3"), col("column_7"), col("column_9")))
                  .withColumn("derived3", concat(col("column_4"), col("column_10"), col("column_21")))
                  .withColumn("derived4", concat(col("column_8"), col("column_36"), col("column_61")))
                  .withColumn("derived5", concat(col("column_9"), col("column_32"), col("column_16")))
                  .withColumn("derived6", concat(col("column_77"), col("column_24"), col("column_63")))
                  .withColumn("derived7", concat(col("column_55"), col("column_36"), col("column_64")))
                  .withColumn("derived8", concat(col("column_28"), col("column_31"), col("column_64")))
                  .withColumn("derived9", concat(col("column_88"), col("column_3"), col("column_10")))
                  .withColumn("derived10", concat(col("column_89"), col("column_36"), col("column_60")))
             )

# COMMAND ----------

sc.setJobDescription("Step H: Wide Table Join with No-Op Write + derived cols")

results_df.write.format("noop").mode("overwrite").save()                

# COMMAND ----------

sc.setJobDescription("Step I: Write Wide Table Join + derived cols")

results_df.write.format("delta").mode("overwrite").save(joined_target_path2)

# COMMAND ----------

# MAGIC %md ## Cleanup

# COMMAND ----------

sc.setJobDescription("Step L: Remove experiment's artifacts")

# Removes all datesets for this experiment
dbutils.fs.rm(target_base_path, True) 