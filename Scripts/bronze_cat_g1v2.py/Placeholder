/*
===========================================================
Loading Cat_g1v2 Data into Bronze Layer
===========================================================
Reading Data from source and writing to Bronze Delta table
============================================================
*/

# read data from source
df = spark.read.csv(r"/Volumes/workspace/bronze_layer/source_system/Source_crp/PX_CAT_G1V2.csv", header=True, inferSchema=True)


# write to Bronze table
df.write.mode("overwrite").saveAsTable("workspace.bronze_layer.PX_CAT_G1V2")
