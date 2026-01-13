# Databricks notebook source
# MAGIC %sql
# MAGIC select * from otc.bronze.src_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from otc.bronze.src_product

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from otc.bronze.src_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from otc.bronze.src_order_items

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from otc.bronze.src_payments

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from otc.bronze.src_shipments

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
# delete from otc.bronze.src_orders;
# delete from otc.bronze.src_order_items;
# delete from otc.bronze.src_payments;
# delete from otc.bronze.src_shipments;

# delete from otc.audit.pipeline_run;

# COMMAND ----------

["src_customer","src_product","src_orders","src_order_items","src_payments","src_shipments","src_fx_rates"]

