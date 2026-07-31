==================================================
Initialization
==================================================

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DataType
from pyspark.sql.functions import col, trim, length


# Read from Bronze Delta Table
df = spark.read.table("workspace.bronze_layer.sales_details")



==================================================
SILVER DATA TRANSFORMATION
==================================================


#RENAME COLUMN NAMES 
RENAME_MAP = {
    "sls_ord_num": "order_number",
    "sls_prd_key": "product_number",
    "sls_cust_id": "customer_id",
    "sls_order_dt": "order_date",
    "sls_ship_dt": "ship_date",
    "sls_due_dt": "due_date",
    "sls_sales": "sales_amount",
    "sls_quantity": "quantity",
    "sls_price": "price"
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)


#TRIMMING LL STRING COLUMNS
for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))


#CLEANING ORDER DATES
df = (
    df
    .withColumn(
        "sls_order_dt",
        F.when(
            (col("sls_order_dt") == 0) | (length(col("sls_order_dt")) != 8),
            None
        ).otherwise(F.to_date(col("sls_order_dt").cast("string"), "yyyyMMdd"))
    )
    .withColumn(
        "sls_ship_dt",
        F.when(
            (col("sls_ship_dt") == 0) | (length(col("sls_ship_dt")) != 8),
            None
        ).otherwise(F.to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd"))
    )
    .withColumn(
        "sls_due_dt",
        F.when(
            (col("sls_due_dt") == 0) | (length(col("sls_due_dt")) != 8),
            None
        ).otherwise(F.to_date(col("sls_due_dt").cast("string"), "yyyyMMdd"))
    )
)


#CORRECTION FOR PRICE AND SALES
df = (
    df
    .withColumn(
        "sls_price",
        F.when(
            (col("sls_price").isNull()) | (col("sls_price") == 0),
            F.when(
                col("sls_quantity") != 0,
                (col("sls_sales") / col("sls_quantity"))
            ).otherwise(None)
        ).otherwise(col("sls_price"))
    )
)

#WRITE TRANSFORMED DATA TO SILVER DELTA TABLE
df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("workspace.silver_layer.Sales_details")
