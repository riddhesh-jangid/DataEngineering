# Databricks notebook source
# MAGIC %sql
# MAGIC select * from otc.bronze.src_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from otc.bronze.src_product

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from otc.audit.pipeline_run

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from otc_config.config.source_schema_registry

# COMMAND ----------

# %sql
# delete from otc.bronze.src_customer;
# delete from otc.bronze.src_product;
# delete from otc.audit.pipeline_run;

# COMMAND ----------


