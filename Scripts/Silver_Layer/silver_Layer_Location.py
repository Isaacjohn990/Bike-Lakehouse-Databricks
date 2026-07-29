#INITIALIZATION
import pyspark.sql.functions as F
from pyspark.sql.types import DataType, StringType
from pyspark.sql.functions import col, trim


# READ FROM BRONZE DELTA TABLE
df = spark.table("workspace.bronze_layer.loc_a101")


===========================================
--- SILVER DATA TRANSFORMATION
============================================

#RENAME COLUMN NAMES FOR EASY READABILITY

RENAME_MAP = {
    "cid": "customer_number",
    "cntry": "country"
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)


#TRIMMING ALL STRING COLUMNS
for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))


#CLEANING AND TRANSFORMATION OF CUSTOMER_ID
df = df.withColumn("CID", F.regexp_replace("CID", "_", " "))

#NORMALIZE THE COUNTRY NAMES
df = df.withColumn(
    "cntry",
    F.when(col("CNTRY") == "DE", "Germany")
     .when(col("CNTRY").isin("US", "USA"), "United States")
     .when((col("CNTRY") == "") | col("CNTRY").isNull(), "n/a")
     .otherwise(col("CNTRY"))
)


#WRITE TRANSFORMED DATA TO SILVER DELTA TABLE
df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver_layer.erp_customer_location")
