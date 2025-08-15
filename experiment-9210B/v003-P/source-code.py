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
# MAGIC     <td>**Standard_L8s**</td>
# MAGIC     <td>**1**</td>
# MAGIC     <td>**8 cores**</td>
# MAGIC     <td>**64 GB**</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>Workers:</td>
# MAGIC     <td>**Standard_L8s**</td>
# MAGIC     <td>**4**</td>
# MAGIC     <td>**16 cores**</td>
# MAGIC     <td>**128 GB**</td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC 
# MAGIC Databricks Runtime: <b>10.1 Photon</b>

# COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")

from pyspark.sql.functions import col, sha2, md5, concat_ws, substring, rand, randn, lit, abs

# Disable the Parquet/Delta IO Cache to avoid side effects
spark.conf.set("spark.databricks.io.cache.enabled", "false")

# Setting shuffle partitions to a high number where only ~800 are really needed
spark.conf.set("spark.sql.shuffle.partitions", 3000)

# Source paths for our wide and lookup dataset
source_base_path = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/experiments/exp-9210"
wide_source_path = f"{source_base_path}/wide_facts.delta"
lookup_source_path = f"{source_base_path}/lookup_dim.delta"

# Target paths for our wide and joined dataset
target_base_path = "dbfs:/dbacademy/experiments/exp-9210B"
dbutils.fs.rm(target_base_path, True) # just in case

wide_target_path = f"{target_base_path}/wide_target.delta"
joined_target_path = f"{target_base_path}/joined_target.delta"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading & Writing Wide Tables w/Photon

# COMMAND ----------

wide_df = spark.read.format("delta").load(wide_source_path)

# COMMAND ----------

sc.setJobDescription("Step D: Read 200-column Delta table with No-Op Write")

wide_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

sc.setJobDescription("Step E: Read & write 200-column Delta table")

wide_df.write.format("delta").mode("overwrite").save(wide_target_path)

# COMMAND ----------

# MAGIC %md ## Joining Wide Tables w/Photon

# COMMAND ----------

lookup_df = spark.read.format("delta").load(lookup_source_path) # Load the lookup table

wide_df = (spark.read.format("delta").load(wide_source_path)    # Load the wide table
              # .hint("skew", "lookup_id")                      # Not required with AQE & Spark 3.x
)
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

# MAGIC %md ## Cleanup

# COMMAND ----------

sc.setJobDescription("Step J: Remove experiment's artifacts")

# Removes all datesets for this experiment
dbutils.fs.rm(target_base_path, True) 