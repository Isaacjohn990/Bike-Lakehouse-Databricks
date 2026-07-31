#INITIALIZATION
import pyspark.sql.functions as F
from  pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col


#READ FROM BRONZE DELTA TABLE
df = spark.table("workspace.bronze_layer.cust_az12")


======================================
SILVER DATA TRANSFORMATION
======================================

#RENAMING COLUMN NAMES 
RENAME_MAP = {
    "cid": "customer_number",
    "bdate": "birth_date",
    "gen": "gender"
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)


#TRIMMING ALL STRING ITEMS
for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))


#CLEANING AND TRANSFORMING CUSTOMER_ID
#...If a customer ID starts with 'NAS', remove the 'NAS' prefix. Otherwise, keep the ID as it is.

df = df.withColumn(
    "cid",
    F.when(col("CID").startswith("NAS"),
           F.substring(col("CID"),4,
    F.length(col("CID"))))
        .otherwise(col("CID"))
)


#VALIDATING & CLEANING DATES
#...checking for invalid future Dates.
df = df.withColumn(
    "bdate",
    F.when(col("bdate") > F.current_date(), None)
     .otherwise(col("bdate"))
)

#NORMALIZING THE GENDER COLUMN

df = df.withColumn(
    "gen",
    F.when(F.upper("gen").startswith("F"), "Female")
     .when(F.upper("gen").startswith("M"), "Male")
     .otherwise("unknown")
)

===============================================
WRITE TRANSFORMED DATA TO SILVER DELTA TABLE
================================================
df.write.mode("overwrite").format("delta").saveAsTable("Silver_layer.cust_az12")
