===============================================
Intialization
===============================================

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DateType
from pyspark.sql.functions import col, trim
from pyspark.sql.window import Window


=============================================
Read From Bronze Delta Table
=============================================

df = spark.read.table("workspace.bronze_layer.prd_info")



=========================================
Silver Data Transformation
========================================

===================================
RENAMING COLUMN NAMES
===================================
RENAME_MAP = {
    "prd_id": "product_id",
    "cat_id": "category_id",
    "prd_key": "product_number",
    "prd_nm": "product_name",
    "prd_cost": "product_cost",
    "prd_line": "product_line",
    "prd_start_dt": "start_date",
    "prd_end_dt": "end_date"
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)


========================================================
TRIMMING ALL STRINGS COLUMNS NAMES FOR EASY READABILITY
========================================================
for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))


======================================================
Product Key Parsing
======================================================
df = df.withColumn("category_id", F.regexp_replace(F.substring(col("product_number"), 1, 5), "-", "_"))
df = df.withColumn("product_number", F.substring(col("product_number"), 7, F.length(col("product_number"))))



=========================================================
Product Normalization
=========================================================

df = (
    df
    # Normalize product line
    .withColumn(
        "product_line",
        F.when(F.upper(col("product_line")) == "M", "Mountain")
         .when(F.upper(col("product_line")) == "R", "Road")
         .when(F.upper(col("product_line")) == "S", "Other Sales")
         .when(F.upper(col("product_line")) == "T", "Touring")
         .otherwise("n/a")
    )
)



=========================================================
write Transformed Data to silver Delta Table
=========================================================

# Write to silver Table
df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("Silver_layer.crm_products")
