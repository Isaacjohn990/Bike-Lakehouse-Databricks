=============================================
--- Intialization
=============================================
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col


=============================================
--- Read From Bronze Delta Table
=============================================
df = spark.read.table("workspace.bronze_layer.cust_info")



=============================================
--- Dictionary for the Column renaming
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
--- Trimming all string Column
=============================================
for field in df.schema.fields:
    if field.dataType == StringType():
        df = df.withColumn(field.name, trim(col(field.name)))



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


=================================================
--- Removing records with mussing Customers ID
=================================================
df = df.filter(col("cst_id").isNotNull())


=======================================================================
--- Cast DataTypes 
=======================================================================

df_typed = df.withColumn("customer_id", col("customer_id").cast("int")) \
              .withColumn("create_date", col("create_date").cast("date"))



=============================================================================================
--- Write Transformed Data to Silver Delta Table
=============================================================================================

df_typed.write.mode("overwrite").format("delta").saveAsTable("workspace.silver_layer.crm_customers")
