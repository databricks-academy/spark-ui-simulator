# Databricks notebook source
sc.setJobDescription("XREF-AKER: Basic initialization")
from pyspark.sql.functions import *
from pyspark.sql import Row
from math import ceil

# Location of our tiny-files datasets
srcPath = "dbfs:/mnt/training/global-sales/transactions/2017.parquet"

# Location of our compacted dataset (p for the python version)
dstPath = "dbfs:/tmp/dbacademy/global-sales/transactions/2017-p.parquet"

# Utility method for fetching and presetning dataset statistics
def showFileStats(path:str):
  fileSizes = [ Row(value=int(f.size/1024)) for f in dbutils.fs.ls(srcPath) if not f.name.startswith("_")]
  df = (spark.createDataFrame(fileSizes)
             .select(count("value"), sum("value")/1024/1024, avg("value")/1024, min("value"), max("value"))
             .toDF("count", "sum GB", "avg MB", "min KB", "max KB"))
  for c in df.columns: df = df.withColumn(c, col(c).cast("decimal(10,2)"))
  display(df)


# COMMAND ----------

sc.setJobDescription("XREF-BDOE: Source File Stats")

showFileStats(srcPath)

# COMMAND ----------

# MAGIC %md
# MAGIC # The Algorithm
# MAGIC 1. Determine the size of your dataset on disk
# MAGIC 2. Decide what your ideal part-file size is
# MAGIC 3. Compute the number of spark-partitions required (divide size-on-disk / ideal-size)
# MAGIC 4. Configure a cluster with N cores *(more cores == less time)*
# MAGIC 5. Read in your data, repartition by N, and then write to disk
# MAGIC 6. Check the Spark UI for spill and any other issues

# COMMAND ----------

sc.setJobDescription("XREF-CKDE: Step 1")
# Determine the size of your dataset on disk

# map(lambda x: x, 'human')

sizeOnDiskMB = __builtin__.sum(  # Sum up all the values
  [f.size for f in               # Convert FileInfo(path,name,size) to just size
   dbutils.fs.ls(srcPath)        # Using DB Utils to list all the source files
   if not f.name.startswith("_") # Filter out the "temp" files
])/1024/1024                     # Convert bytes to MB

displayHTML(f"""Size on Disk: <b>{int(sizeOnDiskMB):,}</b> MB""")

# COMMAND ----------

sc.setJobDescription("XREF-DYEU: Step 2")
# Decide what your ideal part-file size is

# One half of a GB (expressed in MB)
# Should be between 128 MB and 1 GB
targetSizeMB = 1024 / 2

displayHTML(f"""Target Size: <b>{targetSizeMB:,}</b> MB""")

# COMMAND ----------

sc.setJobDescription("XREF-EFUE: Step 3")
# Compute the number of spark-partitions required (divide size-on-disk / ideal-size)

partitalPartitions = sizeOnDiskMB/targetSizeMB
partitions = ceil(partitalPartitions)

displayHTML(f"""<table>
<tr><td>Partial Partitions:</td><td style="text-align:right"><b>{partitalPartitions:,.2f}</b><br/>
<tr><td>Total Partitions:  </td><td style="text-align:right"><b>{partitions:,}</b><br/>
</table>""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Step 4
# MAGIC 
# MAGIC Configure a cluster with N cores (more cores == less time)
# MAGIC * To ingest, we need 203 cores to processes everthing in a single iteration.
# MAGIC * But to write, we only need 3 cores because we have only 3 partitions to write.
# MAGIC 
# MAGIC For this example, we are running with 224 cores to minimize the ingest time:
# MAGIC 
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
# MAGIC     <td>**i3.xlarge**</td>
# MAGIC     <td>**1**</td>
# MAGIC     <td>**4 cores**</td>
# MAGIC     <td>**30.5 GB**</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>Workers:</td>
# MAGIC     <td>**i3.8xlarge**</td>
# MAGIC     <td>**7**</td>
# MAGIC     <td>**224 cores**</td>
# MAGIC     <td>**1708 GB**</td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

sc.setJobDescription("XREF-GPJH: Step 5")
# Read in your data, repartition by N, and then write to disk

sourceDF = (spark
  .read                    # Get the DataFrameReader
  .parquet(srcPath)        # Read in the parquet file
  .repartition(partitions) # One spark-partition per part-file on disk
  .write                   # Get the DataFrameWriter
  .mode("overwrite")       # In case the file already exists
  .parquet(dstPath)        # Write out the parquet file
)

# COMMAND ----------

sc.setJobDescription("XREF-IKEE: Destination File Stats")

showFileStats(dstPath)

# COMMAND ----------

sc.setJobDescription("XREF-JEFW: View Final Result")

display(dbutils.fs.ls(dstPath))

# COMMAND ----------

sc.setJobDescription("XREF-KFOP: Final Clean Up")

dbutils.fs.rm(dstPath, True)