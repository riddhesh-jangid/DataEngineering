# Databricks notebook source
# DBTITLE 1,JBDC Related Things
import requests


# ------------------------------------------------------------
# Defaults (project-wide)
# ------------------------------------------------------------
DEFAULT_SQL_SERVER = "sql-20days.database.windows.net"
DEFAULT_SQL_DB = "free-sql-db-0334595"
DEFAULT_CONTROL_TABLE = "config.data_generation_control"
DEFAULT_UPDATE_PROC = "config.usp_update_data_generation_control"
DEFAULT_AZURE_SQL_SCOPE = "https://database.windows.net/.default"


# ------------------------------------------------------------
# Get Azure AD access token for Azure SQL
# ------------------------------------------------------------
def get_azure_sql_access_token(
    scope: str = DEFAULT_AZURE_SQL_SCOPE,
) -> str:
    tenant_id = dbutils.secrets.get("sp-secrets", "20daysdatabricksSP-tenantID")
    client_id = dbutils.secrets.get("sp-secrets", "20daysdatabricksSP-clientID")
    client_secret = dbutils.secrets.get("sp-secrets", "20daysdatabricksSP-clientSecret")

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": scope,
    }

    resp = requests.post(token_url, data=payload, timeout=30)
    resp.raise_for_status()

    return resp.json()["access_token"]


# ------------------------------------------------------------
# Build JDBC URL for Azure SQL
# ------------------------------------------------------------
def get_azure_sql_jdbc_url(
    sql_server: str = DEFAULT_SQL_SERVER,
    sql_db: str = DEFAULT_SQL_DB,
) -> str:
    return (
        f"jdbc:sqlserver://{sql_server}:1433;"
        f"database={sql_db};"
        "encrypt=true;"
        "trustServerCertificate=false;"
        "hostNameInCertificate=*.database.windows.net;"
        "loginTimeout=30;"
    )


# ------------------------------------------------------------
# JDBC connection properties (token-based auth)
# ------------------------------------------------------------
def get_azure_sql_connection_props(access_token: str) -> dict:
    return {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "accessToken": access_token,
    }


# ------------------------------------------------------------
# Unified JDBC context
# ------------------------------------------------------------
def get_azure_sql_jdbc_context(
    sql_server: str = DEFAULT_SQL_SERVER,
    sql_db: str = DEFAULT_SQL_DB,
) -> dict:
    access_token = get_azure_sql_access_token()
    return {
        "jdbc_url": get_azure_sql_jdbc_url(sql_server, sql_db),
        "connection_props": get_azure_sql_connection_props(access_token),
        "access_token": access_token,
    }


# ------------------------------------------------------------
# Read control table (default: config.data_generation_control)
# ------------------------------------------------------------
def get_data_generation_control_df(
    sql_server: str = DEFAULT_SQL_SERVER,
    sql_db: str = DEFAULT_SQL_DB,
    table_fqn: str = DEFAULT_CONTROL_TABLE,
):
    ctx = get_azure_sql_jdbc_context(sql_server, sql_db)
    return spark.read.jdbc(
        url=ctx["jdbc_url"],
        table=table_fqn,
        properties=ctx["connection_props"],
    )


# ------------------------------------------------------------
# Call stored procedure to update control table
# ------------------------------------------------------------
def update_data_generation_control(
    entity_name: str,
    new_last_generated_num: int,
    updated_at=None,
    sql_server: str = DEFAULT_SQL_SERVER,
    sql_db: str = DEFAULT_SQL_DB,
    procedure_fqn: str = DEFAULT_UPDATE_PROC,
):
    ctx = get_azure_sql_jdbc_context(sql_server, sql_db)

    jvm = spark._sc._gateway.jvm
    conn = None
    stmt = None

    try:
        # Build Java Properties and pass accessToken (THIS is the key fix)
        props = jvm.java.util.Properties()
        props.setProperty("accessToken", ctx["access_token"])
        props.setProperty("encrypt", "true")
        props.setProperty("trustServerCertificate", "false")

        # IMPORTANT: connect with URL + properties
        conn = jvm.java.sql.DriverManager.getConnection(ctx["jdbc_url"], props)

        # Prepare callable statement
        stmt = conn.prepareCall(f"{{CALL {procedure_fqn}(?, ?, ?)}}")
        stmt.setString(1, entity_name)
        stmt.setLong(2, int(new_last_generated_num))

        # Pass NULL to let SQL default SYSUTCDATETIME() (as per your proc)
        if updated_at is None:
            stmt.setObject(3, None)
        else:
            stmt.setString(3, str(updated_at))

        stmt.execute()

    finally:
        if stmt is not None:
            stmt.close()
        if conn is not None:
            conn.close()



# ------------------------------------------------------------
# Helper: convert list[dict] to Spark DataFrame
# ------------------------------------------------------------
def _records_to_df(records: List[Dict[str, Any]]) -> DataFrame:
    if not records:
        raise ValueError("records cannot be empty")
    return spark.createDataFrame(records)


# ------------------------------------------------------------
# Insert src_customer into Azure SQL
# ------------------------------------------------------------
def insert_src_customer(
    customers: List[Dict[str, Any]],
    sql_server: str = DEFAULT_SQL_SERVER,
    sql_db: str = DEFAULT_SQL_DB,
    table_fqn: str = "config.src_customer",
):
    df = _records_to_df(customers)

    ctx = get_azure_sql_jdbc_context(sql_server, sql_db)

    (
        df.write
        .mode("append")
        .jdbc(
            url=ctx["jdbc_url"],
            table=table_fqn,
            properties=ctx["connection_props"],
        )
    )


# ------------------------------------------------------------
# Insert src_product into Azure SQL
# ------------------------------------------------------------
def insert_src_product(
    products: List[Dict[str, Any]],
    sql_server: str = DEFAULT_SQL_SERVER,
    sql_db: str = DEFAULT_SQL_DB,
    table_fqn: str = "config.src_product",
):
    df = _records_to_df(products)

    ctx = get_azure_sql_jdbc_context(sql_server, sql_db)

    (
        df.write
        .mode("append")
        .jdbc(
            url=ctx["jdbc_url"],
            table=table_fqn,
            properties=ctx["connection_props"],
        )
    )

# COMMAND ----------

# DBTITLE 1,landing write

def write_csv_landing_data(table_name: str, sources_data: dict):
    list_of_dict = sources_data["raw"][table_name]
    last_max_id = sources_data["max_last_ids"][table_name]

    run_date = datetime.now().strftime("%Y-%m-%d")

    output_path = (
        f"dbfs:/Volumes/otc/volumn/landingfiles/{table_name}/"
        f"run_date={run_date}"
    )

    dbutils.fs.mkdirs(output_path.replace("dbfs:", ""))

    df = spark.createDataFrame(list_of_dict)
    df.coalesce(1).write.mode("append").option("header", True).csv(output_path)

    update_data_generation_control(table_name, last_max_id)
    return output_path


def write_json_landing_data(table_name: str, sources_data: dict):
    records = sources_data["raw"][table_name]
    last_max_id = sources_data["max_last_ids"][table_name]

    run_date = datetime.now().strftime("%Y-%m-%d")

    output_path = (
        f"dbfs:/Volumes/otc/volumn/landingfiles/{table_name}/"
        f"run_date={run_date}"
    )

    dbutils.fs.mkdirs(output_path.replace("dbfs:", ""))

    # convert records to JSON lines DataFrame correctly
    json_rows = [(json.dumps(r, ensure_ascii=False),) for r in records]
    df = spark.createDataFrame(json_rows, ["value"])

    df.coalesce(1).write.mode("append").text(output_path)

    update_data_generation_control(table_name, last_max_id)
    return output_path

# COMMAND ----------

# DBTITLE 1,get_control_table_params
def get_control_table_params(
    data_generation_control_df: DataFrame,
    table_name: str,
) -> Dict[str, int]:
    row = (
        data_generation_control_df
        .filter(F.col("entity_name") == table_name)
        .select("last_generated_num", "min_id_num", "max_id_num")
        .limit(1)
        .collect()
    )

    if not row:
        raise ValueError(f"No entry found in data_generation_control for table '{table_name}'")

    r = row[0]
    return {
        "last_generated_num": int(r["last_generated_num"]),
        "min_id_num": int(r["min_id_num"]),
        "max_id_num": int(r["max_id_num"]),
    }

# COMMAND ----------

# DBTITLE 1,_last_num_from_ids
import re

def _last_num_from_ids(ids, prefix: str) -> int:
    if not ids:
        raise ValueError("ids cannot be empty")
    mx = -1
    pat = re.compile(rf"^{re.escape(prefix)}(\d+)$")
    for x in ids:
        m = pat.match(x)
        if not m:
            raise ValueError(f"Bad id '{x}' for prefix '{prefix}'")
        mx = max(mx, int(m.group(1)))
    return mx

# COMMAND ----------

# DBTITLE 1,get data_generation_control
data_generation_control = get_data_generation_control_df()
data_generation_control.display()

# COMMAND ----------

# DBTITLE 1,generate_src_customer
def generate_src_customer(
    no_of_rows: int,
    seed: int | None = 42,
) -> List[Dict[str, Any]]:
    
    (last_generated_num, min_id_num, max_id_num) = get_control_table_params(data_generation_control ,'src_customer').values()

    if not isinstance(last_generated_num, int):
        raise ValueError("last_generated_num must be an integer")
    if not isinstance(no_of_rows, int) or no_of_rows <= 0:
        raise ValueError("no_of_rows must be a positive integer")
    if not isinstance(min_id_num, int) or not isinstance(max_id_num, int):
        raise ValueError("min_id_num and max_id_num must be integers")
    if min_id_num <= 0 or max_id_num <= 0:
        raise ValueError("min_id_num and max_id_num must be positive")
    if min_id_num > max_id_num:
        raise ValueError("min_id_num must be <= max_id_num")

    # last_generated_num is the last *absolute* ID number generated.
    # next IDs must be sequential: last_generated_num+1, last_generated_num+2, ...
    # enforce bounds using min_id_num/max_id_num.
    if last_generated_num < (min_id_num - 1):
        # if table is "fresh", allow last_generated_num to be below min-1, but normalize start
        last_generated_num = min_id_num - 1

    next_start = last_generated_num + 1
    next_end = last_generated_num + no_of_rows

    if next_start < min_id_num:
        raise ValueError(
            f"Next ID start {next_start} is below min_id_num {min_id_num}. "
            f"Check last_generated_num={last_generated_num}."
        )
    if next_end > max_id_num:
        raise ValueError(
            f"Not enough ID capacity. Next end {next_end} exceeds max_id_num {max_id_num} "
            f"(last_generated_num={last_generated_num}, requested={no_of_rows})."
        )

    rng = random.Random(seed)

    first_names = [
        "Aarav", "Vihaan", "Aditya", "Arjun", "Reyansh", "Vivaan", "Krishna", "Ishaan",
        "Aanya", "Ananya", "Diya", "Ira", "Myra", "Siya", "Anika", "Kavya",
        "Rohan", "Rahul", "Kunal", "Siddharth", "Aditi", "Isha", "Riya", "Nisha",
    ]
    last_names = [
        "Sharma", "Verma", "Gupta", "Mehta", "Patel", "Khan", "Singh", "Kumar",
        "Iyer", "Nair", "Reddy", "Jain", "Agarwal", "Chopra", "Malhotra", "Bose",
    ]
    cities = [
        "Bengaluru", "Hyderabad", "Pune", "Mumbai", "Delhi", "Chennai", "Kolkata",
        "Ahmedabad", "Jaipur", "Indore", "Surat", "Nagpur", "Lucknow", "Coimbatore",
    ]

    now = datetime.now(timezone.utc)
    rows: List[Dict[str, Any]] = []

    for n in range(next_start, next_end + 1):
        cid = f"C{n}"

        first = rng.choice(first_names)
        last = rng.choice(last_names)
        full_name = f"{first} {last} {cid}"

        city = rng.choice(cities)

        updated_at = now - timedelta(days=rng.randint(0, 365), minutes=rng.randint(0, 24 * 60))

        ingest_ts = now

        rows.append(
            {
                "customer_id": cid,
                "full_name": full_name,
                "city": city,
                "updated_at": updated_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "ingest_ts": ingest_ts.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            }
        )

    return rows

# COMMAND ----------

# DBTITLE 1,generate_src_product
def generate_src_product(
    no_of_rows: int,
    seed: int | None = 42,
) -> List[Dict[str, Any]]:

    (last_generated_num, min_id_num, max_id_num) = get_control_table_params(data_generation_control ,'src_product').values()

    if not isinstance(last_generated_num, int):
        raise ValueError("last_generated_num must be an integer")
    if not isinstance(no_of_rows, int) or no_of_rows <= 0:
        raise ValueError("no_of_rows must be a positive integer")
    if not isinstance(min_id_num, int) or not isinstance(max_id_num, int):
        raise ValueError("min_id_num and max_id_num must be integers")
    if min_id_num > max_id_num:
        raise ValueError("min_id_num must be <= max_id_num")

    # ---- ID generation (ID is the center) ----
    next_start = last_generated_num + 1
    next_end = last_generated_num + no_of_rows

    if next_start < min_id_num:
        raise ValueError(
            f"Next product_id {next_start} is below min_id_num {min_id_num}"
        )

    if next_end > max_id_num:
        raise ValueError(
            f"ID overflow: next_end {next_end} exceeds max_id_num {max_id_num}"
        )

    rng = random.Random(seed)

    categories = [
        "Electronics", "Home", "Kitchen", "Fashion", "Beauty",
        "Sports", "Books", "Toys", "Grocery", "Automotive",
    ]

    product_bases = {
        "Electronics": ["Phone", "Laptop", "Headphones", "Smartwatch", "Tablet"],
        "Home": ["Lamp", "Chair", "Table", "Curtains"],
        "Kitchen": ["Mixer", "Cookware Set", "Kettle", "Knife Set"],
        "Fashion": ["T-Shirt", "Jeans", "Jacket", "Sneakers"],
        "Beauty": ["Face Wash", "Moisturizer", "Shampoo", "Sunscreen"],
        "Sports": ["Dumbbells", "Yoga Mat", "Football", "Cricket Bat"],
        "Books": ["Novel", "Notebook", "Planner", "Textbook"],
        "Toys": ["Puzzle", "RC Car", "Board Game", "Action Figure"],
        "Grocery": ["Olive Oil", "Honey", "Oats", "Almonds"],
        "Automotive": ["Car Charger", "Seat Cover", "Phone Mount", "Vacuum Cleaner"],
    }

    brands = [
        "Astra", "Nexa", "Zenith", "Orion", "Nova", "Atlas", "Auro",
    ]

    now = datetime.now(timezone.utc)
    rows: List[Dict[str, Any]] = []

    for n in range(next_start, next_end + 1):
        product_id = f"P{n}"

        category = rng.choice(categories)
        base = rng.choice(product_bases[category])
        brand = rng.choice(brands)
        model = rng.randint(100, 9999)

        product_name = f"{brand} {base} {model} {product_id}"

        list_price = Decimal(
            str(rng.uniform(99, 9999))
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        is_active = rng.random() < 0.9

        updated_at = now - timedelta(
            days=rng.randint(0, 365),
            minutes=rng.randint(0, 1440),
        )
        ingest_ts = now

        rows.append(
            {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "list_price": str(list_price),
                "is_active": is_active,
                "updated_at": updated_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "ingest_ts": ingest_ts.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            }
        )

    return rows

# COMMAND ----------

# DBTITLE 1,generate_purchase
def generate_purchase(
    no_of_rows: int,
    customers: Sequence[Dict[str, Any]],
    products: Sequence[Dict[str, Any]],
    seed: int | None = 42,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Minimal, controlled transactional generator aligned to your decisions:

    - Orders: insert-only, order_status always "CONFIRMED"
    - Payments: insert-only, payment_status always "SUCCESS"
      - Still includes nested JSON via `payment_payload` (struct + array).
      - If multiple attempts are generated, failures come first and SUCCESS is the final attempt.
      - amount is STRING on purpose.
    - Shipments: the only table with meaningful lifecycle variation ("CREATED" or "DELIVERED")
      - This table will be your primary target for MERGE/update practice later.
    - ID generation: sequential & bounded via data_generation_control for each entity.

    Output:
      {
        "src_orders": [...],
        "src_order_items": [...],
        "src_payments": [...],
        "src_shipments": [...]
      }
    """

    if not isinstance(no_of_rows, int) or no_of_rows <= 0:
        raise ValueError("no_of_rows must be a positive integer")
    if not customers:
        raise ValueError("customers cannot be empty")
    if not products:
        raise ValueError("products cannot be empty")

    rng = random.Random(seed)
    now = datetime.now(timezone.utc)

    def _iso_z(dt: datetime) -> str:
        return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _allocate_ids(entity_name: str, id_prefix: str, count: int) -> List[str]:
        params = get_control_table_params(data_generation_control, entity_name)
        last_generated_num = int(params["last_generated_num"])
        min_id_num = int(params["min_id_num"])
        max_id_num = int(params["max_id_num"])

        next_start = last_generated_num + 1
        next_end = last_generated_num + count

        if next_start < min_id_num:
            raise ValueError(f"[{entity_name}] Next ID start {next_start} is below min_id_num {min_id_num}")
        if next_end > max_id_num:
            raise ValueError(f"[{entity_name}] ID overflow: next_end {next_end} exceeds max_id_num {max_id_num}")

        return [f"{id_prefix}{n}" for n in range(next_start, next_end + 1)]

    # Master keys for selection
    customer_ids = [c["customer_id"] for c in customers]
    product_ids = [p["product_id"] for p in products]

    product_price_map: Dict[str, str] = {}
    for p in products:
        product_price_map[p["product_id"]] = str(p.get("list_price", "0"))

    # Allocate IDs
    order_ids = _allocate_ids("src_orders", "O", no_of_rows)

    items_per_order = [rng.randint(1, 4) for _ in range(no_of_rows)]
    total_items = sum(items_per_order)

    order_item_ids = _allocate_ids("src_order_items", "OI", total_items)
    payment_event_ids = _allocate_ids("src_payments", "PMT", no_of_rows)     # 1 payment record per order
    shipment_event_ids = _allocate_ids("src_shipments", "SHP", no_of_rows)   # 1 shipment record per order

    # Statuses (simplified)
    ORDER_STATUS = "CONFIRMED"
    PAYMENT_STATUS = "SUCCESS"
    shipment_statuses = ["CREATED", "DELIVERED"]

    orders: List[Dict[str, Any]] = []
    order_items: List[Dict[str, Any]] = []
    payments: List[Dict[str, Any]] = []
    shipments: List[Dict[str, Any]] = []

    oi_cursor = 0

    for i, order_id in enumerate(order_ids):
        customer_id = rng.choice(customer_ids)

        # Order time (business time)
        order_ts = now - timedelta(days=rng.randint(0, 30), minutes=rng.randint(0, 24 * 60))
        updated_at = order_ts + timedelta(minutes=rng.randint(0, 24 * 60))
        ingest_ts = now

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_ts": _iso_z(order_ts),
                "order_status": ORDER_STATUS,
                "updated_at": _iso_z(updated_at),
                "ingest_ts": _iso_z(ingest_ts),
            }
        )

        # Order items
        k_items = items_per_order[i]
        chosen_products = rng.sample(product_ids, k=min(k_items, len(product_ids)))
        while len(chosen_products) < k_items:
            chosen_products.append(rng.choice(product_ids))

        for j in range(k_items):
            order_item_id = order_item_ids[oi_cursor]
            oi_cursor += 1

            product_id = chosen_products[j]
            quantity = rng.randint(1, 5)

            lp = Decimal(product_price_map.get(product_id, "0") or "0")
            if lp <= 0:
                lp = Decimal("499.00")

            unit_price = (lp * Decimal(str(rng.uniform(0.85, 1.05)))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )

            item_updated_at = order_ts + timedelta(minutes=rng.randint(0, 24 * 60))
            item_ingest_ts = now

            order_items.append(
                {
                    "order_id": order_id,
                    "order_item_id": order_item_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": str(unit_price),
                    "updated_at": _iso_z(item_updated_at),
                    "ingest_ts": _iso_z(item_ingest_ts),
                }
            )

        # Payment: nested JSON in `payment_payload`
        pmt_id = payment_event_ids[i]

        # total amount based on items just created for this order
        order_item_slice = order_items[oi_cursor - k_items : oi_cursor]
        total_amount = sum(
            Decimal(it["unit_price"]) * Decimal(str(it["quantity"]))
            for it in order_item_slice
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        payment_ts = order_ts + timedelta(minutes=rng.randint(1, 180))
        payment_ingest_ts = now

        # Attempts: failures first, SUCCESS last (always)
        attempt_count = rng.randint(1, 5)
        # If only 1 attempt => SUCCESS directly
        attempts: List[Dict[str, Any]] = []
        base_attempt_ts = payment_ts - timedelta(minutes=max(0, attempt_count - 1) * rng.randint(1, 5))

        if attempt_count == 1:
            attempts.append(
                {
                    "attempt_no": 1,
                    "attempt_ts": _iso_z(payment_ts),
                    "result": "SUCCESS",
                    "failure_reason": None,
                }
            )
        else:
            # failures
            for a in range(1, attempt_count):
                attempts.append(
                    {
                        "attempt_no": a,
                        "attempt_ts": _iso_z(base_attempt_ts + timedelta(minutes=a * rng.randint(1, 5))),
                        "result": "FAILED",
                        "failure_reason": rng.choice(["NETWORK", "BANK_TIMEOUT", "INSUFFICIENT_FUNDS"]),
                    }
                )
            # final success
            attempts.append(
                {
                    "attempt_no": attempt_count,
                    "attempt_ts": _iso_z(payment_ts),
                    "result": "SUCCESS",
                    "failure_reason": None,
                }
            )

        payments.append(
            {
                "payment_event_id": pmt_id,
                "order_id": order_id,
                "payment_ts": _iso_z(payment_ts),
                "payment_status": PAYMENT_STATUS,   # simplified: always SUCCESS
                "amount": str(total_amount),        # STRING on purpose
                "ingest_ts": _iso_z(payment_ingest_ts),
                "payment_payload": {
                    "method": rng.choice(["CARD", "UPI", "NETBANKING", "WALLET"]),
                    "currency": "INR",
                    "gateway": rng.choice(["Razorpay", "PayU", "Stripe"]),
                    "attempts": attempts,  # ARRAY[STRUCT]
                },
            }
        )

        # Shipment: status is either CREATED or DELIVERED (simplified)
        shp_id = shipment_event_ids[i]

        shipment_ts = order_ts + timedelta(hours=rng.randint(6, 96))
        shipment_ingest_ts = now

        shipment_status = rng.choice(shipment_statuses)

        shipments.append(
            {
                "shipment_event_id": shp_id,
                "order_id": order_id,
                "shipment_ts": _iso_z(shipment_ts),
                "shipment_status": shipment_status,
                "ingest_ts": _iso_z(shipment_ingest_ts),
            }
        )

    return {
        "src_orders": orders,
        "src_order_items": order_items,
        "src_payments": payments,
        "src_shipments": shipments,
    }

# COMMAND ----------

# DBTITLE 1,generate_sources backup
# ------------------------------------------------------------
# Helper: extract max numeric part from generated IDs (e.g., O3000001 -> 3000001)
# ------------------------------------------------------------
def _max_numeric_id(records: List[Dict[str, Any]], id_col: str, prefix: str) -> int:
    if not records:
        raise ValueError(f"records for '{id_col}' cannot be empty")

    pat = re.compile(rf"^{re.escape(prefix)}(\d+)$")
    mx = -1

    for r in records:
        v = r.get(id_col)
        if v is None:
            raise ValueError(f"Missing '{id_col}' in record: {r}")

        m = pat.match(str(v))
        if not m:
            raise ValueError(f"Bad id '{v}' for prefix '{prefix}' in column '{id_col}'")

        mx = max(mx, int(m.group(1)))

    if mx < 0:
        raise ValueError(f"Could not compute max numeric id for '{id_col}'")

    return mx


# ------------------------------------------------------------
# Helper: prepare last_generated_num updates
# ------------------------------------------------------------
def _compute_last_ids_all_tables(
    customers: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
    purchase_payload: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, int]:
    out: Dict[str, int] = {}

    out["src_customer"] = _max_numeric_id(customers, "customer_id", "C")
    out["src_product"] = _max_numeric_id(products, "product_id", "P")

    out["src_orders"] = _max_numeric_id(purchase_payload["src_orders"], "order_id", "O")
    out["src_order_items"] = _max_numeric_id(purchase_payload["src_order_items"], "order_item_id", "OI")
    out["src_payments"] = _max_numeric_id(purchase_payload["src_payments"], "payment_event_id", "PMT")
    out["src_shipments"] = _max_numeric_id(purchase_payload["src_shipments"], "shipment_event_id", "SHP")

    return out


# ------------------------------------------------------------
# Main: generate raw source records + update control table
# ------------------------------------------------------------
def generate_sources(
    no_of_customer: int = 20,
    no_of_product: int = 10,
    no_of_purchase: int = 50,
    seed: int | None = 42,
) -> Dict[str, Any]:
    """
    End-to-end helper:
      1) Read data_generation_control from Azure SQL
      2) Generate raw source records:
           - src_customer (sequential IDs via control table)
           - src_product  (simple fixed generator; ignores control)
           - purchase bundle (src_orders, src_order_items, src_payments, src_shipments)
      3) Compute max numeric IDs used for transactional tables
      4) Update data_generation_control via stored procedure for those transactional entities

    Params:
      no_of_customer: number of new customer records to generate
      no_of_product: number of products to generate (note: current generate_src_product() returns 250 fixed;
                     this param is accepted for orchestration, but you can adjust generate_src_product() later)
      no_of_purchase: number of orders to generate (purchase creates dependent records too)
      seed: deterministic randomness

    Returns:
      {
        "raw": {
          "src_customer": [...],
          "src_product": [...],
          "src_orders": [...],
          "src_order_items": [...],
          "src_payments": [...],
          "src_shipments": [...]
        },
        "max_last_ids": {
          "src_orders": <int>,
          "src_order_items": <int>,
          "src_payments": <int>,
          "src_shipments": <int>
        }
      }
    """

    # ----------------------------
    # Validate inputs
    # ----------------------------
    if not isinstance(no_of_customer, int) or no_of_customer <= 0:
        raise ValueError("no_of_customer must be a positive integer")
    if not isinstance(no_of_product, int) or no_of_product <= 0:
        raise ValueError("no_of_product must be a positive integer")
    if not isinstance(no_of_purchase, int) or no_of_purchase <= 0:
        raise ValueError("no_of_purchase must be a positive integer")

    # ----------------------------
    # Load control table (Spark DF)
    # ----------------------------
    data_generation_control = get_data_generation_control_df()

    # ----------------------------
    # Generate master data (raw records)
    # ----------------------------
    # NOTE: generate_src_customer uses get_control_table_params(data_generation_control, 'src_customer')
    customers = generate_src_customer(no_of_rows=no_of_customer, seed=seed)

    # NOTE: current product generator returns 250 fixed rows, ignoring no_of_product.
    # If you later want dynamic product count, modify generate_src_product() accordingly.
    products = generate_src_product(no_of_rows=no_of_customer, seed=seed)

    # ----------------------------
    # Generate purchases (raw records) and segregate into 4 tables
    # ----------------------------
    purchase_payload = generate_purchase(
        no_of_rows=no_of_purchase,
        customers=customers,
        products=products,
        seed=seed,
    )

    # ----------------------------
    # Compute max IDs used in this batch for transactional tables
    # ----------------------------
    max_last_ids = _compute_last_ids_all_tables(customers, products, purchase_payload)


    # ----------------------------
    # Final output: raw records + max ID state used
    # ----------------------------
    raw_out = {
        "src_customer": customers,
        "src_product": products,
        "src_orders": purchase_payload["src_orders"],
        "src_order_items": purchase_payload["src_order_items"],
        "src_payments": purchase_payload["src_payments"],
        "src_shipments": purchase_payload["src_shipments"],
    }

    return {
        "raw": raw_out,
        "max_last_ids": max_last_ids,
    }
