# Databricks notebook source
# DBTITLE 1,Init
# from pyspark.sql import Row

# data = [Row(ID=i, name=f"Name_{i}") for i in range(1, 11)]
# df = spark.createDataFrame(data)
# df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/findout/deltaoverview")

# COMMAND ----------

# DBTITLE 1,Append
# from pyspark.sql import Row

# new_data = [Row(ID=i, name=f"Name_{i}") for i in range(11, 16)]
# df_new = spark.createDataFrame(new_data)
# df_new.write.format("delta").mode("append").save("dbfs:/FileStore/findout/deltaoverview")

# COMMAND ----------

# DBTITLE 1,Delete
# from delta.tables import DeltaTable

# delta_table = DeltaTable.forPath(spark, "dbfs:/FileStore/findout/deltaoverview")
# delta_table.delete("ID IN (11, 12, 13, 14)")

# COMMAND ----------

df_parquet = spark.read.format('delta').load("dbfs:/FileStore/findout/deltaoverview")
display(df_parquet)

# COMMAND ----------

# DBTITLE 1,parquetfiles
# parquet_files = [f.path for f in dbutils.fs.ls("dbfs:/FileStore/findout/parquetfiles") if f.path.endswith(".parquet")]

# for file_path in parquet_files:
#     df = spark.read.format("parquet").load(file_path)
#     display(df)

# COMMAND ----------

# DBTITLE 1,temp when appended
# parquet_files = [f.path for f in dbutils.fs.ls("dbfs:/FileStore/findout/temp") if f.path.endswith(".parquet")]

# for file_path in parquet_files:
#     print(file_path)
#     df = spark.read.format("parquet").load(file_path)
#     display(df)

# COMMAND ----------

# DBTITLE 1,temp when deleted
parquet_files = [f.path for f in dbutils.fs.ls("dbfs:/FileStore/findout/temp") if f.path.endswith(".parquet")]

for file_path in parquet_files:
    print(file_path)
    df = spark.read.format("parquet").load(file_path)
    display(df)

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "dbfs:/FileStore/findout/deltaoverview")
display(delta_table.history())

# COMMAND ----------

# DBTITLE 1,Append
# modified_files = [f.path for f in dbutils.fs.ls("dbfs:/FileStore/findout/deltaoverview") if f.path.endswith(".parquet")]
# old_files = [f.path for f in dbutils.fs.ls("dbfs:/FileStore/findout/parquetfiles")]

# modified_filenames = set([f.split("/")[-1] for f in modified_files])
# old_filenames = set([f.split("/")[-1] for f in old_files])

# common_files = modified_filenames & old_filenames
# only_in_modified = modified_filenames - old_filenames

# print("Common files:", list(common_files))
# print("Files only in modified:", list(only_in_modified))

# for filename in only_in_modified:
#     src = f"dbfs:/FileStore/findout/deltaoverview/{filename}"
#     dst = f"dbfs:/FileStore/findout/temp/{filename}"
#     dbutils.fs.cp(src, dst)

# COMMAND ----------

['part-00002-2beedb98-73bd-4d48-9184-f6d5d14162d7-c000.snappy.parquet', 
 'part-00000-8a07d6ad-ba99-4a8d-961b-0c40729df307-c000.snappy.parquet', 
 'part-00003-3f7247f3-4d8c-4c6f-ad9f-0364dc2e0355-c000.snappy.parquet', 
 'part-00001-c4632344-3f2a-4bd8-9e6b-20c98e25433e-c000.snappy.parquet']

 ['part-00002-e488c7ce-f8f7-4736-835e-06fad8d18141-c000.snappy.parquet', 
  'part-00001-ac737dcb-9c4b-4016-a05c-d972981faa47-c000.snappy.parquet', 
  'part-00003-d9bbab22-12f4-4567-a61a-aa8ff6964cd2-c000.snappy.parquet', 
  'part-00000-b106127f-28c5-46fa-8531-dc8d2583883e-c000.snappy.parquet']

# COMMAND ----------

# DBTITLE 1,Delete
modified_files = [f.path for f in dbutils.fs.ls("dbfs:/FileStore/findout/deltaoverview") if f.path.endswith(".parquet")]
old_files = [f.path for f in dbutils.fs.ls("dbfs:/FileStore/findout/parquetfiles") if f.path.endswith(".parquet")]

modified_filenames = set([f.split("/")[-1] for f in modified_files])
old_filenames = set([f.split("/")[-1] for f in old_files])

common_files = modified_filenames & old_filenames
only_in_modified = modified_filenames - old_filenames

print("Common files:", list(common_files))
print("Files only in modified:", list(only_in_modified))

for filename in only_in_modified:
    src = f"dbfs:/FileStore/findout/deltaoverview/{filename}"
    dst = f"dbfs:/FileStore/findout/temp/{filename}"
    dbutils.fs.cp(src, dst)

# COMMAND ----------

[
    'part-00003-d9bbab22-12f4-4567-a61a-aa8ff6964cd2-c000.snappy.parquet', 
    'part-00000-b106127f-28c5-46fa-8531-dc8d2583883e-c000.snappy.parquet', 
    'part-00002-e488c7ce-f8f7-4736-835e-06fad8d18141-c000.snappy.parquet', 
    'part-00002-2beedb98-73bd-4d48-9184-f6d5d14162d7-c000.snappy.parquet', 
    'part-00001-ac737dcb-9c4b-4016-a05c-d972981faa47-c000.snappy.parquet', 
    'part-00000-8a07d6ad-ba99-4a8d-961b-0c40729df307-c000.snappy.parquet', 
    'part-00003-3f7247f3-4d8c-4c6f-ad9f-0364dc2e0355-c000.snappy.parquet', 
    'part-00001-c4632344-3f2a-4bd8-9e6b-20c98e25433e-c000.snappy.parquet']

['part-00000-a3728537-0dcb-4893-9fd7-8ad1e9b2866a-c000.snappy.parquet']
