/*
--- Loading Sales details info Data into Bronze Layer

===========================================================
Reading Data from source and writing to Bronze Delta table
============================================================
*/

# read data from source
df = spark.read.csv(r"/Volumes/workspace/bronze_layer/source_system/Source_crm/sales_details (1).csv", header=True, inferSchema=True)
display(df)


# write to Bronze table
df.write.mode("overwrite").saveAsTable("workspace.bronze_layer.sales_details")
