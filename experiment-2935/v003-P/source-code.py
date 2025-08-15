# Databricks notebook source
# MAGIC %md
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <td></td>
# MAGIC       <td>VM</td>
# MAGIC       <td>Quantity</td>
# MAGIC       <td>Total Cores</td>
# MAGIC       <td>Total RAM</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC       <td>Driver:</td>
# MAGIC       <td>**Standard_DS12_v2**</td>
# MAGIC       <td>**1**</td>
# MAGIC       <td>**4 cores**</td>
# MAGIC       <td>**28.0 GB**</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC       <td>Workers:</td>
# MAGIC       <td>**Standard_DS12_v2**</td>
# MAGIC       <td>**4**</td>
# MAGIC       <td>**16 cores**</td>
# MAGIC       <td>**112 GB**</td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")
from pyspark.sql.functions import *

spark.conf.set("spark.databricks.io.cache.enabled", "false")

# 100 GBs of data, partitioned by year
data_path_part = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb-par_year.delta"

# Same 100 GB of data, not optimized for any type of filtering
data_path_not_part = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb.delta"

# The city we will use in our tests
target_city = 365900539

# COMMAND ----------

sc.setJobDescription("Step B: No predicates")

# No filter applied
(spark
   .read.format("delta").load(data_path_not_part) # Load the delta table
   .write.format("noop").mode("overwrite").save() # Execute a noop write to test
)

# COMMAND ----------

sc.setJobDescription("Step C: Predicate filter")

# Not optimized for any type of filtering
(spark
  .read.format("delta").load(data_path_not_part) # Load the delta table
  .where(col("city_id") == target_city)          # Filter by target city
  .where(year(col("transacted_at")) == 2013)     # Filter by the computed year
  .write.format("noop").mode("overwrite").save() # Execute a noop write to test
)

# COMMAND ----------

sc.setJobDescription("Step D: Partition filter")

# Dataset is partitioned by transaction year
(spark
  .read.format("delta").load(data_path_part)     # Load the partitioned, delta table  
  .where(col("city_id") == target_city)          # Filter by target city
  .where(col("p_transacted_year") == 2013)       # Filter by the partitioned year
  .write.format("noop").mode("overwrite").save() # Execute a noop write to test
)

# COMMAND ----------

sc.setJobDescription("Step E: Filter propogation")

# Filter can be pushed down even through shuffle operation
(spark
  .read.format("delta").load(data_path_part)     # Load the partitioned, delta table
  .groupBy("city_id").agg(sum(col("amount")))    # Aggregate by city_id and sum the amount
  .where(col("city_id") == target_city)          # Filter by target city
  .write.format("noop").mode("overwrite").save() # Execute a noop write to test
)

# COMMAND ----------

sc.setJobDescription("Step F: Partition filter not applied")

# Although dataset is partitoined, no utilization of partitioning
(spark
  .read.format("delta").load(data_path_part)     # Load the partitioned, delta table
  .where(col('city_id') == target_city)          # Filter by target city
  .where(year(col("transacted_at")) == 2013)     # Filter by the computed year
  .write.format("noop").mode("overwrite").save() # Execute a noop write to test
)

# COMMAND ----------

sc.setJobDescription("Step G: Filter on sum column")

# Filter on aggregate column is not split into filters on compoments columns
(spark
  .read.format("delta").load(data_path_part)          # Load the partitioned, delta table
  .withColumn("tax", col("amount")*0.2)               # Add a new column (calculating tax)
  .withColumn("total",  col("amount") + col("tax"))   # Add a new column (calculating total)
  .where(col("total") > 400)                          # Filter by newly created columns
  .write.format("noop").mode("overwrite").save()      # Execute a noop write to test
)

# COMMAND ----------

sc.setJobDescription("Step H: Crippling the Predicate")

# Extra cache in between prevents filters pushdown
(spark
  .read.format("delta").load(data_path_part)     # Load the partitioned, delta table
  .cache()                                       # Cache the dataset imeadiately after read
  .where(col("city_id") == target_city)          # Filter by target city
  .where(col("p_transacted_year") == 2013)       # Filter by the partitioned year
  .write.format("noop").mode("overwrite").save() # Execute a noop write to test
)