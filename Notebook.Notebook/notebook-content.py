# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8ff9da31-76e3-417f-9b47-2616293fec0c",
# META       "default_lakehouse_name": "Lakehouse",
# META       "default_lakehouse_workspace_id": "959bf169-7bd5-4462-8070-0dcab9eb4d7e",
# META       "known_lakehouses": [
# META         {
# META           "id": "8ff9da31-76e3-417f-9b47-2616293fec0c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Load packages

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create a table with all important dates and their attributes.

# CELL ********************

date_range = (
    spark.sql("SELECT explode(sequence(to_date('2008-01-01'), to_date('2026-12-31'), interval 1 day)) AS date_key")
    .withColumn("year", F.year("date_key"))
    .withColumn("quarter", F.quarter("date_key"))
    .withColumn("month", F.month("date_key"))
    .withColumn("month_name", F.monthname("date_key"))
    .withColumn("day_of_week", F.dayofweek("date_key"))
    .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
    .withColumn("season", 
        F.when(F.col("month").isin([12, 1, 2]), "Winter")
        .when(F.col("month").isin([3, 4, 5]), "Spring")
        .when(F.col("month").isin([6, 7, 8]), "Summer")
        .when(F.col("month").isin([9, 10, 11]), "Fall")
    )
)

date_range.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("silver.date_range")

df = spark.sql("SELECT * FROM Lakehouse.silver.date_range LIMIT 15")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform FX USD/EUR table

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
