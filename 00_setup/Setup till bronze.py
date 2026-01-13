# Databricks notebook source
# DBTITLE 1,EL landingfiles
CREDENTIAL = "20daysdbx_stg_credential"
EL_PATH = "abfss://landingfiles@stgsourceadls.dfs.core.windows.net"
EL_NAME = "landingfiles"

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS {EL_NAME}
URL '{EL_PATH}'
WITH (CREDENTIAL {CREDENTIAL})
COMMENT 'Location for landing files'
""")

# COMMAND ----------

# DBTITLE 1,EL externalfiles
CREDENTIAL = "20daysdbx_stg_credential"
EL_PATH = "abfss://externalfiles@stgsourceadls.dfs.core.windows.net"
EL_NAME = "externalfiles"

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS {EL_NAME}
URL '{EL_PATH}'
WITH (CREDENTIAL {CREDENTIAL})
COMMENT 'Location for external files'
""")

# COMMAND ----------

# DBTITLE 1,EL managedcontainer
CREDENTIAL = "20daysdbx_stg_credential"
EL_PATH = "abfss://managedcontainer@stgfor20databricks.dfs.core.windows.net"
EL_NAME = "managedcontainer"

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS {EL_NAME}
URL '{EL_PATH}'
WITH (CREDENTIAL {CREDENTIAL})
COMMENT 'Location for external files'
""")

# COMMAND ----------

# DBTITLE 1,EL managedconfigcontainer
CREDENTIAL = "20daysdbx_stg_credential"
EL_PATH = "abfss://managedconfigcontainer@stgfor20databricks.dfs.core.windows.net"
EL_NAME = "managedconfigcontainer"

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS {EL_NAME}
URL '{EL_PATH}'
WITH (CREDENTIAL {CREDENTIAL})
COMMENT 'Location for external files'
""")

# COMMAND ----------

# DBTITLE 1,Create Catalog otc
CL_MANAGED_PATH = "abfss://managedcontainer@stgfor20databricks.dfs.core.windows.net"

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS otc
MANAGED LOCATION '{CL_MANAGED_PATH}'
""")

# COMMAND ----------

# DBTITLE 1,create catalog otc_config
CL_MANAGED_CONFIG_PATH = "abfss://managedconfigcontainer@stgfor20databricks.dfs.core.windows.net"

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS otc_config
MANAGED LOCATION '{CL_MANAGED_CONFIG_PATH}'
""")

# COMMAND ----------

# DBTITLE 1,volumn schema otc
spark.sql("""
CREATE SCHEMA IF NOT EXISTS otc.volumn
COMMENT 'Volume for files'
""")

# COMMAND ----------

# DBTITLE 1,audit schema
spark.sql("""
CREATE SCHEMA IF NOT EXISTS otc.audit
COMMENT 'Schema for audit tables'
""")

# COMMAND ----------

# DBTITLE 1,config schema otc_config
spark.sql("""
CREATE SCHEMA IF NOT EXISTS otc_config.config
COMMENT 'Managed storage for config'
""")

# COMMAND ----------

# DBTITLE 1,landingfiles volumn
VOLUMN_EL = 'abfss://landingfiles@stgsourceadls.dfs.core.windows.net'
VOLUMN_NAME = 'landingfiles'
VOLUMN_CATALOG = 'otc'
VOLUMN_SCHEMA = 'volumn'

spark.sql(f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {VOLUMN_CATALOG}.{VOLUMN_SCHEMA}.{VOLUMN_NAME}
LOCATION '{VOLUMN_EL}'
""")

# COMMAND ----------

# DBTITLE 1,externalfiles volumn
VOLUMN_EL = 'abfss://externalfiles@stgsourceadls.dfs.core.windows.net'
VOLUMN_NAME = 'externalfiles'
VOLUMN_CATALOG = 'otc'
VOLUMN_SCHEMA = 'volumn'

spark.sql(f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {VOLUMN_CATALOG}.{VOLUMN_SCHEMA}.{VOLUMN_NAME}
LOCATION '{VOLUMN_EL}'
""")

# COMMAND ----------

# DBTITLE 1,external folders for bronze table
# Minimal & strict UC Volume folder creation (idempotent)

CATALOG = "otc"
SCHEMA = "volumn"
VOLUME = "externalfiles"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
PROJECT_ROOT = f"{VOLUME_ROOT}/"

LAYERS = ["bronze"]

BRONZE_SOURCES = [
    "src_customer",
    "src_product",
    "src_orders",
    "src_order_items",
    "src_payments",
    "src_shipments",
    "src_fx_rates",
]

def mkdir(path: str):
    try:
        dbutils.fs.mkdirs(path)   # idempotent in UC Volumes
        print(f"OK   : {path}")
    except Exception as e:
        print(f"FAIL : {path} | {e}")

# project root
mkdir(PROJECT_ROOT)

# layer folders
for layer in LAYERS:
    mkdir(f"{PROJECT_ROOT}/{layer}")

# bronze source folders ONLY (no assumptions)
for src in BRONZE_SOURCES:
    mkdir(f"{PROJECT_ROOT}/bronze/{src}")

# COMMAND ----------

# DBTITLE 1,delete externalfiles volumn
# deleting externalfiles volumn as it would be needed for tables creation
spark.sql("DROP VOLUME IF EXISTS otc.volumn.externalfiles")

# COMMAND ----------

# DBTITLE 1,creating empty bronze tables
# ============================================================
# CONFIG
# ============================================================

# Unity Catalog target schema for Bronze (authoritative in doc)
CATALOG = "otc"
UC_SCHEMA = "bronze"  # DOC: bronze.<table> e.g.,bronze.src_customer

# ADLS Gen2 base (external)
STORAGE_ACCOUNT = "stgsourceadls"
CONTAINER = "externalfiles"

# Root folder in ADLS that matches your existing folder hierarchy
# (This corresponds to your earlier Volume path /bronze/...)
ADLS_ROOT = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_ROOT = f"{ADLS_ROOT}/bronze"

# ============================================================
# TABLE LOCATION MAP
# ============================================================

table_locations = {
    "src_customer":    f"{BRONZE_ROOT}/src_customer",
    "src_product":     f"{BRONZE_ROOT}/src_product",
    "src_orders":      f"{BRONZE_ROOT}/src_orders",
    "src_order_items": f"{BRONZE_ROOT}/src_order_items",
    "src_payments":    f"{BRONZE_ROOT}/src_payments",
    "src_shipments":   f"{BRONZE_ROOT}/src_shipments",
    "src_fx_rates":    f"{BRONZE_ROOT}/src_fx_rates",
}

# ============================================================
# BRONZE SCHEMAS (AUTHORITATIVE FROM DOC)
# - run_date and source_file are system-added in Bronze for all tables
# - payments.amount is STRING on purpose
# - payments.payment_payload is nested (STRUCT/ARRAY) per doc; we store as STRING to keep Bronze raw/stable.
# ============================================================

bronze_columns_ddl = {
    "src_customer": """
        customer_id STRING,
        full_name STRING,
        city STRING,
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP,
        run_date DATE,
        source_file STRING
    """,
    "src_product": """
        product_id STRING,
        product_name STRING,
        category STRING,
        list_price DECIMAL(10,2),
        is_active BOOLEAN,
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP,
        run_date DATE,
        source_file STRING
    """,
    "src_orders": """
        order_id STRING,
        customer_id STRING,
        order_ts TIMESTAMP,
        order_status STRING,
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP,
        run_date DATE,
        source_file STRING
    """,
    "src_order_items": """
        order_id STRING,
        order_item_id STRING,
        product_id STRING,
        quantity INT,
        unit_price DECIMAL(10,2),
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP,
        run_date DATE,
        source_file STRING
    """,
    "src_payments": """
        payment_event_id STRING,
        order_id STRING,
        payment_ts TIMESTAMP,
        payment_status STRING,
        amount STRING,               -- doc: STRING on purpose
        payment_payload STRING,       -- doc: STRUCT/ARRAY; stored raw as STRING in Bronze
        ingest_ts TIMESTAMP,
        run_date DATE,
        source_file STRING
    """,
    "src_shipments": """
        shipment_event_id STRING,
        order_id STRING,
        shipment_ts TIMESTAMP,
        shipment_status STRING,
        ingest_ts TIMESTAMP,
        run_date DATE,
        source_file STRING
    """,
    "src_fx_rates": """
        currency STRING,
        rate DECIMAL(10,4),
        rate_date DATE,
        ingest_ts TIMESTAMP,
        run_date DATE,
        source_file STRING
    """,
}

# ============================================================
# CREATE SCHEMA
# ============================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{UC_SCHEMA}")

# ============================================================
# CREATE EXTERNAL TABLES (IDEMPOTENT)
# ============================================================

for table_name,location in table_locations.items():
    full_name = f"{CATALOG}.{UC_SCHEMA}.{table_name}"
    cols = bronze_columns_ddl[table_name].strip()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name} (
            {cols}
        )
        USING DELTA
        PARTITIONED BY (run_date)
        LOCATION '{location}'
    """)

    print(f"OK : {full_name} -> {location}")

print("DONE: Bronze external Delta tables created in UC schema `bronze`.")


# COMMAND ----------

# DBTITLE 1,source_schema_registry
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS otc_config.config.source_schema_registry (
# MAGIC   table_name         STRING,
# MAGIC   format             STRING,
# MAGIC   schema_ddl          STRING,
# MAGIC   read_options_json   STRING,
# MAGIC   nullable_columns    STRING,
# MAGIC   primary_key         STRING,
# MAGIC   notes              STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC INSERT INTO otc_config.config.source_schema_registry
# MAGIC VALUES
# MAGIC -- src_customer (CSV)
# MAGIC (
# MAGIC   'src_customer',
# MAGIC   'csv',
# MAGIC   'customer_id STRING,full_name STRING,city STRING,updated_at TIMESTAMP,ingest_ts TIMESTAMP',
# MAGIC   '{"header":"true","delimiter":",","mode":"FAILFAST"}',
# MAGIC   '["city"]',
# MAGIC   'customer_id',
# MAGIC   'Landing CSV extract from Azure SQL (raw rows). Bronze adds run_date (partition) and source_file (lineage).'
# MAGIC ),
# MAGIC
# MAGIC -- src_product (CSV)
# MAGIC (
# MAGIC   'src_product',
# MAGIC   'csv',
# MAGIC   'product_id STRING,product_name STRING,category STRING,list_price DECIMAL(10,2),is_active BOOLEAN,updated_at TIMESTAMP,ingest_ts TIMESTAMP',
# MAGIC   '{"header":"true","delimiter":",","mode":"FAILFAST"}',
# MAGIC   '[]',
# MAGIC   'product_id',
# MAGIC   'Landing CSV extract from Azure SQL (raw rows). Bronze adds run_date (partition) and source_file (lineage).'
# MAGIC ),
# MAGIC
# MAGIC -- src_orders (CSV)
# MAGIC (
# MAGIC   'src_orders',
# MAGIC   'csv',
# MAGIC   'order_id STRING,customer_id STRING,order_ts TIMESTAMP,order_status STRING,updated_at TIMESTAMP,ingest_ts TIMESTAMP',
# MAGIC   '{"header":"true","delimiter":",","mode":"FAILFAST"}',
# MAGIC   '[]',
# MAGIC   'order_id',
# MAGIC   'Landing raw CSV drop. Bronze adds run_date (partition) and source_file (lineage).'
# MAGIC ),
# MAGIC
# MAGIC -- src_order_items (CSV)
# MAGIC (
# MAGIC   'src_order_items',
# MAGIC   'csv',
# MAGIC   'order_id STRING,order_item_id STRING,product_id STRING,quantity INT,unit_price DECIMAL(10,2),updated_at TIMESTAMP,ingest_ts TIMESTAMP',
# MAGIC   '{"header":"true","delimiter":",","mode":"FAILFAST"}',
# MAGIC   '[]',
# MAGIC   'order_item_id',
# MAGIC   'Landing raw CSV drop. Bronze adds run_date (partition) and source_file (lineage).'
# MAGIC ),
# MAGIC
# MAGIC -- src_payments (JSON - nested)
# MAGIC (
# MAGIC   'src_payments',
# MAGIC   'json',
# MAGIC   'payment_event_id STRING,order_id STRING,payment_ts TIMESTAMP,payment_status STRING,amount STRING,payment_payload STRING,ingest_ts TIMESTAMP',
# MAGIC   '{"multiLine":"false"}',
# MAGIC   '[]',
# MAGIC   'payment_event_id',
# MAGIC   'Landing nested JSON (as-is). Amount is STRING on purpose. Bronze retains nested payload; exact struct depends on JSON. Bronze adds run_date (partition) and source_file (lineage).'
# MAGIC ),
# MAGIC
# MAGIC -- src_shipments (JSON)
# MAGIC (
# MAGIC   'src_shipments',
# MAGIC   'json',
# MAGIC   'shipment_event_id STRING,order_id STRING,shipment_ts TIMESTAMP,shipment_status STRING,ingest_ts TIMESTAMP',
# MAGIC   '{"multiLine":"false"}',
# MAGIC   '[]',
# MAGIC   'shipment_event_id',
# MAGIC   'Landing JSON events (as-is). Bronze adds run_date (partition) and source_file (lineage).'
# MAGIC ),
# MAGIC
# MAGIC -- src_fx_rates (REST JSON)
# MAGIC (
# MAGIC   'src_fx_rates',
# MAGIC   'json',
# MAGIC   'currency STRING,rate DECIMAL(10,4),rate_date DATE,ingest_ts TIMESTAMP',
# MAGIC   '{"multiLine":"false"}',
# MAGIC   '[]',
# MAGIC   'currency,rate_date',
# MAGIC   'REST API JSON (as-is). Bronze adds run_date (partition) and source_file (lineage).'
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,pipeline_run & bronze_file_log
# MAGIC %sql
# MAGIC
# MAGIC -- 1) Run-level audit table
# MAGIC CREATE TABLE IF NOT EXISTS otc.audit.pipeline_run (
# MAGIC   pipeline_name      STRING,
# MAGIC   pipeline_run_id    STRING,
# MAGIC   table_name         STRING,
# MAGIC   run_date           DATE,
# MAGIC   run_mode           STRING,       -- single | multiple
# MAGIC   start_ts           TIMESTAMP,
# MAGIC   end_ts             TIMESTAMP,
# MAGIC   status             STRING,       -- success | failed
# MAGIC   rows_read          BIGINT,
# MAGIC   error_message      STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,creating empty silver tables
# ============================================================
# CONFIG
# ============================================================

CATALOG = "otc"
UC_SCHEMA = "silver"   # Managed tables under otc.silver

# ============================================================
# SILVER SCHEMAS (AUTHORITATIVE FROM DOC)
# - Managed tables (NO explicit LOCATION)
# - No IDENTITY columns (surrogate keys populated by ingestion logic, not table DDL)
# ============================================================

silver_columns_ddl = {
    "customer": """
        customer_sk BIGINT,
        customer_id STRING,
        full_name STRING,
        city STRING,
        effective_from DATE,
        effective_to DATE,
        is_current BOOLEAN,
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP
    """,
    "product": """
        product_sk BIGINT,
        product_id STRING,
        product_name STRING,
        category STRING,
        list_price DECIMAL(10,2),
        is_active BOOLEAN,
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP
    """,
    "orders": """
        order_id STRING,
        customer_id STRING,
        order_ts TIMESTAMP,
        order_status STRING,
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP
    """,
    "order_items": """
        order_item_id STRING,
        order_id STRING,
        product_id STRING,
        quantity INT,
        unit_price DECIMAL(10,2),
        updated_at TIMESTAMP,
        ingest_ts TIMESTAMP
    """,
    "payments": """
        payment_event_id STRING,
        order_id STRING,
        payment_ts TIMESTAMP,
        payment_status STRING,
        amount DECIMAL(10,2),
        ingest_ts TIMESTAMP
    """,
    "shipments": """
        shipment_event_id STRING,
        order_id STRING,
        shipment_ts TIMESTAMP,
        shipment_status STRING,
        ingest_ts TIMESTAMP
    """,
}

# ============================================================
# CREATE SCHEMA
# ============================================================

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{UC_SCHEMA}")

# ============================================================
# CREATE MANAGED SILVER TABLES (IDEMPOTENT)
# ============================================================

for table_name, cols in silver_columns_ddl.items():
    full_name = f"{CATALOG}.{UC_SCHEMA}.{table_name}"
    cols = cols.strip()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name} (
            {cols}
        )
        USING DELTA
    """)

    print(f"OK : {full_name}")

print("DONE: Silver managed Delta tables created in UC schema `silver`.")


# COMMAND ----------

# DBTITLE 1,creating watermark table
CATALOG = "otc"
UC_SCHEMA = "silver"   # Managed tables under otc.silver

spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.{UC_SCHEMA}.watermark (
  table_name STRING,
  watermark_ts TIMESTAMP
)""")

spark.sql(f"""
          MERGE INTO {CATALOG}.{UC_SCHEMA}.watermark as w
          USING (
            SELECT STACK(6, 'customer', 'product', 'orders', 'order_items', 'payments', 'shipments') as table_name
            ) AS s
            ON w.table_name = s.table_name
            WHEN NOT MATCHED THEN 
              INSERT (table_name, watermark_ts) VALUES (s.table_name, TIMESTAMP '1900-01-01T00:00:00.000+00:00')
          """)

# COMMAND ----------


