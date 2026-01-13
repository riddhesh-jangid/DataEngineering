# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# DBTITLE 1,Imports
import requests
from __future__ import annotations

from datetime import datetime, timezone, timedelta
import random
from typing import List, Dict, Any, Iterable
from decimal import Decimal, ROUND_HALF_UP

from typing import Dict, Any, List, Optional, Sequence, Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

import json

# COMMAND ----------

# DBTITLE 1,Run setup notebook
# MAGIC %run ./data_generation_control_setup

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("no_of_customer", "20", "Number of Customers")
dbutils.widgets.text("no_of_product", "10", "Number of Products")
dbutils.widgets.text("no_of_purchase", "50", "Number of Purchases")

no_of_customer = int(dbutils.widgets.get("no_of_customer"))
no_of_product = int(dbutils.widgets.get("no_of_product"))
no_of_purchase = int(dbutils.widgets.get("no_of_purchase"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generation

# COMMAND ----------

sources_data = generate_sources()

# COMMAND ----------

# DBTITLE 1,customer landing
customer_list_of_dict = sources_data['raw']['src_customer']
customer_last_max_id = sources_data['max_last_ids']['src_customer']
insert_src_customer(customer_list_of_dict)
update_data_generation_control("src_customer", customer_last_max_id)

# COMMAND ----------

# DBTITLE 1,product landing
product_list_of_dict = sources_data['raw']['src_product']
product_last_max_id = sources_data['max_last_ids']['src_product']
insert_src_product(product_list_of_dict)
update_data_generation_control("src_product", product_last_max_id)

# COMMAND ----------

# DBTITLE 1,CSV landing write
write_csv_landing_data('src_orders', sources_data)
write_csv_landing_data('src_order_items', sources_data)

# COMMAND ----------

# DBTITLE 1,JSON landing write
write_json_landing_data('src_payments', sources_data)
write_json_landing_data('src_shipments', sources_data)
