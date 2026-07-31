=============================================
 Intialization
=============================================
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col



=============================================
Read From Bronze Delta Table
=============================================
df = spark.read.table("workspace.bronze_layer.cust_info")



=============================================
Dictionary for the Column renaming
=============================================
RENAME_MAP = {
    "cst_id": "customer_id",
    "cst_key": "customer_key",
    "cst_firstname": "customer_first_name",
    "cst_lastname": "customer_last_name",
    "cst_marital_status": "marital_status",
    "cst_gndr": "gender",
    "cst_create_date": "create_date"
}
for old_name, new_name in RENAME_COLUMNS.items():
    df = df.withColumnRenamed(old_name, new_name)


=============================================
--- Normalization
=============================================
# Marital Status mapping
marital_map = (
    F.when(F.upper(col("cst_marital_status")) == "S", "Single")
    .when(F.upper(col("cst_marital_status")) == "M", "Married")
    .otherwise("N/a")
)


# Gender mapping
gender_map = (
        F.when(F.upper(col("cst_gndr")) == "M", "male")
         .when(F.upper(col("cst_gndr")) == "F", "female")
         .otherwise("n/a")
)  

df =(
    df
    .withColumn("cst_marital_status", marital_map)
    .withColumn("cst_gndr", gender_map)
)


# Rename columns
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)
df = df.withColumnRenamed(old_name, new_name)


=============================================
--- Trimming all string Column
=============================================
# Trim all string columns
df = (
    df.withColumn("customer_key", trim(col("customer_key")))
      .withColumn("customer_first_name", trim(col("customer_first_name")))
      .withColumn("customer_last_name", trim(col("customer_last_name")))
      .withColumn("marital_status", trim(col("marital_status")))
      .withColumn("gender", trim(col("gender")))
)


=================================================
--- Removing records with mussing Customers ID
=================================================
df = df.filter(col("cst_id").isNotNull())



=============================================================================================
Write Transformed Data to Silver Delta Table
=============================================================================================

df.write.mode("overwrite").format("delta").saveAsTable("Silver_layer.crm_customers")
