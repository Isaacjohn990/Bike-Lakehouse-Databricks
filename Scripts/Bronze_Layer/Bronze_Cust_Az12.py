

#LOADING CUST_AZ12 INTO BRONZE LAYER FROM THE SOURCE.
===========================================================
Reading Data from source and writing to Bronze Delta table
============================================================

# read data from source
df = spark.read.csv(r"/Volumes/workspace/bronze_layer/source_system/Source_crp/CUST_AZ12.csv", header=True, inferSchema=True)


# write to Bronze table
df.write.mode("overwrite").saveAsTable("workspace.bronze_layer.CUST_AZ12")
