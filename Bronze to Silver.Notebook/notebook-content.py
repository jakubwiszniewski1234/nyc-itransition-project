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

%pip install geopandas shapely

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import pyspark.sql.types as T
import geopandas as gpd
import shapely as shp


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def show_null_count(df):
    df.select([F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]).show()

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

df_fx = spark.table("bronze.USD_EUR_Daily_Rates")
display(df_fx.limit(5))

df_fx = df_fx.join(date_range, df_fx["fx_date"] == date_range["date_key"], "inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fx = df_fx.select(
    F.col("fx_date").cast("date"),
    F.col("usd_per_eur"),
    F.col("CURRENCY").alias("currency"),
    F.col("CURRENCY_DENOM").alias("currency_denom"),
    F.col("is_ecb_trading_day"),
    F.col("eur_per_usd")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Unique values: {df_fx[["fx_date"]].distinct().count()}")
display(df_fx.summary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fx.write.format("delta").mode("overwrite").saveAsTable("silver.fx_currency_rates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform World Bank GDP Data

# CELL ********************

df_gdp = spark.table("bronze.WorldBankGDP")
display(df_gdp.limit(10))
print(f"Rows: {df_gdp.count()}")
display(df_gdp.printSchema())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gdp = df_gdp.select(
    F.col("country_iso3"),
    F.col("country_name"),
    F.col("year"),
    F.col("gdp_usd")
).filter(F.col("year").between(2008, 2026))

show_null_count(df_gdp)
display(df_gdp.summary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gdp.write.format("delta").mode("overwrite").saveAsTable("silver.world_bank_gdp")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform OpenAQ Air Quality Data

# CELL ********************

df_aq = spark.table("bronze.OpenAQ_Daily_Measurements")
display(df_aq.limit(5))
print(f"Rows: {df_aq.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_aq.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

show_null_count(df_aq)
print(df_aq.count())
display(df_aq[df_aq["locality"].isNull()])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_aq = Window.partitionBy(["date_utc", "sensor_id", "parameter"]).orderBy(F.col("percent_complete").desc())
df_aq = df_aq.withColumn("rn", F.row_number().over(window_aq)).filter(F.col("rn") == 1).drop("rn")
display(df_aq)
print(df_aq.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_aq[df_aq["daily_avg"].isNull()])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_aq.groupBy("parameter").count().show()
df_aq.groupBy("units").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import matplotlib.pyplot as plt
import numpy as np

b, c = df_aq.select("percent_complete").rdd.flatMap(lambda x: x).histogram(25)
plt.bar(
    b[:-1],
    c,
    width = b[1] - b[0] - 0.5
)
plt.title("Histogram of percentage completion")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_aq[["sensor_id", "date_utc", "parameter"]].distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_aq.select(["parameter", "daily_min", "daily_avg", "daily_max"]).dropna().filter(F.col("parameter") == "o3").summary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Sometimes the values are set to 0. It's a very rare case, could be something like a temporary sensor error. We should drop these occurances.
# 
# **Edit**. They are not, there could be just too low level of substances in the air.

# CELL ********************

df_aq_o3 = df_aq.select(["sensor_id", "date_utc", "parameter", "daily_min", "daily_avg", "daily_max", "units"]).filter(F.col("parameter") == "o3")
display(df_aq_o3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

peaks = df_aq_o3.groupBy("date_utc").agg(F.max("daily_max").alias("peak")).orderBy(F.col("peak"), ascending=False)
peaks_avg = df_aq_o3.groupBy("date_utc").agg(F.max("daily_avg").alias("peak_avg")).orderBy(F.col("peak_avg"), ascending=False)
drops = df_aq_o3.groupBy("date_utc").agg(F.min("daily_min").alias("drops")).orderBy(F.col("drops"), ascending=False)

peaks_chrono = peaks.orderBy(F.col("date_utc")).toPandas()
peaks_avg_chrono = peaks_avg.orderBy(F.col("date_utc")).toPandas()
drops_chrono = drops.orderBy(F.col("date_utc")).toPandas()

plt.figure(figsize=(12, 5))
plt.plot(
    peaks_chrono["date_utc"],
    peaks_chrono["peak"],
    color="#e95014ff",
    alpha = 0.5,
    linewidth=1.2,
)
plt.plot(
    peaks_avg_chrono["date_utc"],
    peaks_avg_chrono["peak_avg"],
    color="#4d66e2ff",
    linewidth=1.2,
)
plt.plot(
    drops_chrono["date_utc"],
    drops_chrono["drops"],
    color="#5bd419ff",
    linewidth=1.2,
)
plt.title("Daily O3 in NYC (min, avg, max)")
plt.xlabel("Date")
plt.ylabel("Daily measures (ppm)")
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_aq.select(["parameter", "daily_min", "daily_avg", "daily_max"]).dropna().filter(F.col("parameter") == "pm25").summary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_aq_pm25[["units"]].groupBy("units").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Negative values of pm25 are sometimes considered a "very-clean" air due to high vulnerability of some sensors to environment noise but we will round them up to 0.
# We can see there are outliers - pm25 at very, very high levels, we need to identify and describe them to see if they're not errors.

# CELL ********************

df_aq = (
    df_aq
    .filter(F.col("daily_avg") >= -10.0)
    .withColumn("daily_avg", F.greatest(F.col("daily_avg"), F.lit(0.0)))
    .withColumn(
        "daily_min",
        F.when(F.col("daily_min") < 0.0, F.lit(0.0))
         .otherwise(F.col("daily_min"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_aq_pm25 = df_aq.select(["sensor_id", "location_name", "date_utc", "parameter", "daily_min", "daily_avg", "daily_max", "units"]).dropna().filter(F.col("parameter") == "pm25")
display(df_aq_pm25.groupBy("date_utc").agg(F.max("daily_max").alias("peak")).orderBy(F.col("peak"), ascending=False))
peaks = df_aq_pm25.groupBy("date_utc").agg(F.max("daily_max").alias("peak")).orderBy(F.col("peak"), ascending=False)
peaks_avg = df_aq_pm25.groupBy("date_utc").agg(F.max("daily_avg").alias("peak_avg")).orderBy(F.col("peak_avg"), ascending=False)
drops = df_aq_pm25.groupBy("date_utc").agg(F.min("daily_min").alias("drops")).orderBy(F.col("drops"), ascending=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

peaks = df_aq_pm25.groupBy("date_utc").agg(F.max("daily_max").alias("peak")).orderBy(F.col("peak"), ascending=False)
peaks_avg = df_aq_pm25.groupBy("date_utc").agg(F.max("daily_avg").alias("peak_avg")).orderBy(F.col("peak_avg"), ascending=False)
drops = df_aq_pm25.groupBy("date_utc").agg(F.min("daily_min").alias("drops")).orderBy(F.col("drops"), ascending=False)

peaks_chrono = peaks.orderBy(F.col("date_utc")).toPandas()
peaks_avg_chrono = peaks_avg.orderBy(F.col("date_utc")).toPandas()
drops_chrono = drops.orderBy(F.col("date_utc")).toPandas()

plt.figure(figsize=(12, 5))
plt.plot(
    peaks_chrono["date_utc"],
    peaks_chrono["peak"],
    color="#e95014ff",
    alpha = 0.5,
    linewidth=1.2,
)
plt.plot(
    peaks_avg_chrono["date_utc"],
    peaks_avg_chrono["peak_avg"],
    color="#4d66e2ff",
    linewidth=1.2,
)
plt.plot(
    drops_chrono["date_utc"],
    drops_chrono["drops"],
    color="#5bd419ff",
    linewidth=1.2,
)
plt.title("Daily PM2.5 in NYC (min, avg, max)")
plt.xlabel("Date")
plt.ylabel("Peak Daily Max (µg/m³)")
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# There are several dates that regularly show up with high daily_max. Explanations of them are as follows:
# - On the 4th of July in 2021, 2022, 2023, 2024, 2025, 2026 there were big spikes in air pollution by PM2.5 measure, most likely due to fireworks lit that day. The biggest air quality drop was encountered in 2023.
# - The spikes on 7th and 8th of June are results of Canadian wildfires that created a yellow-ish smog across NYC and other places -- the air quality was bad.
# - The spike on 16th July 2026 occurred agaain due to Canadian wildfires
# 
# Generally, there are no visible malfunctions of sensors as far as daily peaks are analysed.
# 
# Let's detect outliers by IQR method.

# CELL ********************

df_aq_no2 = df_aq.select(["sensor_id", "location_name", "date_utc", "parameter", "daily_min", "daily_avg", "daily_max", "units"]).filter(F.col("parameter") == "no2")
display(df_aq_no2.summary())
df_aq_no2.groupBy("units").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_aq_no2.groupBy("date_utc").agg(F.max("daily_avg").alias("peak_avg")).orderBy(F.col("peak_avg"), ascending=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

peaks = df_aq_no2.groupBy("date_utc").agg(F.max("daily_max").alias("peak")).orderBy(F.col("peak"), ascending=False)
peaks_avg = df_aq_no2.groupBy("date_utc").agg(F.max("daily_avg").alias("peak_avg")).orderBy(F.col("peak_avg"), ascending=False)
drops = df_aq_no2.groupBy("date_utc").agg(F.min("daily_min").alias("drops")).orderBy(F.col("drops"), ascending=False)

peaks_chrono = peaks.orderBy(F.col("date_utc")).toPandas()
peaks_avg_chrono = peaks_avg.orderBy(F.col("date_utc")).toPandas()
drops_chrono = drops.orderBy(F.col("date_utc")).toPandas()

plt.figure(figsize=(12, 5))
plt.plot(
    peaks_chrono["date_utc"],
    peaks_chrono["peak"],
    color="#e95014ff",
    alpha = 0.5,
    linewidth=1.2,
)
plt.plot(
    peaks_avg_chrono["date_utc"],
    peaks_avg_chrono["peak_avg"],
    color="#4d66e2ff",
    linewidth=1.2,
)
plt.plot(
    drops_chrono["date_utc"],
    drops_chrono["drops"],
    color="#5bd419ff",
    linewidth=1.2,
)
plt.title("Daily NO2 in NYC (min, avg, max)")
plt.xlabel("Date")
plt.ylabel("Daily measures (ppm)")
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_aq.select(["sensor_id", "location_id", "date_utc", "parameter", "daily_avg", "daily_min", "daily_max", "units", "percent_complete", "latitude", "longitude"]).write.format("delta").mode("overwrite").saveAsTable("silver.air_quality_measurements")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transforming Taxi tables

# CELL ********************

df_taxi_yellow = spark.read.table("bronze.yellow_taxi_trips_2010_2026")
df_taxi_yellow.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df_taxi_yellow.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_yellow = df_taxi_yellow.select(
    F.to_utc_timestamp(F.col("tpep_pickup_datetime").cast("timestamp"), "America/New_York").alias("pickup_datetime_utc"),
    F.to_utc_timestamp(F.col("tpep_dropoff_datetime").cast("timestamp"), "America/New_York").alias("dropoff_datetime_utc"),
    F.col("trip_distance").cast("double"),
    F.col("pickup_longitude").cast("double"),
    F.col("pickup_latitude").cast("double"),
    F.col("dropoff_longitude").cast("double"),
    F.col("dropoff_latitude").cast("double"),
    F.col("PULocationID").cast("int").alias("pickup_location_id"),
    F.col("DOLocationID").cast('int').alias("dropoff_location_id"),
    F.col("total_amount").cast("double").alias("total_amount_usd"),
    F.lit("yellow").alias("taxi_type")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Some locations are written as lat/long and some as location ids. We need to convert them into location IDs.
# 
# We're going to use spacial data available at NYC TLC website.

# CELL ********************

geospacial_rows = df_taxi_yellow.filter(F.col("pickup_location_id").isNull())
zone_rows = df_taxi_yellow.filter(F.col("pickup_location_id").isNotNull())

unique_coords = (
    geospacial_rows
    .select(
        F.round(F.col("pickup_longitude").cast("double"), 4).alias("lon"),
        F.round(F.col("pickup_latitude").cast("double"), 4).alias("lat")
    )
    .filter(F.col("lon").isNotNull() & F.col("lat").isNotNull() & (F.col("lon") != 0.0) & (F.col("lat") != 0.0))
    .distinct()
    .toPandas()
)


taxi_zones = gpd.read_file("/lakehouse/default/Files/taxi_zones/taxi_zones.shp").to_crs("EPSG:4326")[["LocationID", "geometry"]]
points_gdf = gpd.GeoDataFrame(
    unique_coords,
    geometry=gpd.points_from_xy(unique_coords["lon"], unique_coords["lat"]),
    crs="EPSG:4326"
)

lookup_pdf = (
    gpd.sjoin(points_gdf, taxi_zones, how="inner", predicate="within")[["lon", "lat", "LocationID"]]
    .drop_duplicates(subset=["lon", "lat"])
)

lookup_sdf = F.broadcast(spark.createDataFrame(lookup_pdf))

mapped = (
    geospacial_rows
    .withColumn("pu_lon_r", F.round(F.col("pickup_longitude").cast("double"), 4))
    .withColumn("pu_lat_r", F.round(F.col("pickup_latitude").cast("double"), 4))
    .join(lookup_sdf, (F.col("pu_lon_r") == F.col("lon")) & (F.col("pu_lat_r") == F.col("lat")), "left")
    .withColumn("pickup_location_id", F.col("LocationID").cast("int"))
    .drop("pu_lon_r", "pu_lat_r", "lon", "lat", "LocationID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_yellow = (
    zone_rows
    .unionByName(mapped)
    .fillna({"pickup_location_id": 264, "dropoff_location_id": 264})
    .drop("pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_taxi_yellow.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

show_null_count(df_taxi_yellow)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_yellow.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_yellow = df_taxi_yellow.drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_yellow = df_taxi_yellow.filter(df_taxi_yellow["total_amount_usd"] > 0)
df_taxi_yellow.write.format("delta").mode("overwrite").saveAsTable("silver.taxi_trips")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transform green taxi data

# CELL ********************

df_taxi_green = spark.read.table("bronze.green_taxi_trips")
df_taxi_green.count()
df_taxi_green.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_green = df_taxi_green.select(
    F.to_utc_timestamp(F.col("lpep_pickup_datetime").cast("timestamp"), "America/New_York").alias("pickup_datetime_utc"),
    F.to_utc_timestamp(F.col("lpep_dropoff_datetime").cast("timestamp"), "America/New_York").alias("dropoff_datetime_utc"),
    F.col("trip_distance").cast("double"),
    F.col("PULocationID").cast("int").alias("pickup_location_id"),
    F.col("DOLocationID").cast('int').alias("dropoff_location_id"),
    F.col("total_amount").cast("double").alias("total_amount_usd"),
    F.lit("green").alias("taxi_type")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

show_null_count(df_taxi_green)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df_taxi_green.count())
df_taxi_green = df_taxi_green.drop_duplicates()
print(df_taxi_green.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_green = df_taxi_green.filter(df_taxi_green["total_amount_usd"] > 0)
df_taxi_green = df_taxi_green.filter(df_taxi_green["trip_distance"] >= 0)
display(df_taxi_green.summary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_taxi_green.orderBy("total_amount_usd", ascending=False).limit(50))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_taxi_green
    .filter(F.col("total_amount_usd") > 200)
    .withColumn(
        "fare_bucket",
        F.when(F.col("total_amount_usd") <= 350, "$200 - $350")
         .when(F.col("total_amount_usd") <= 600, "$350 - $600")
         .otherwise("> $600")
    )
    .groupBy("fare_bucket")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.percentile_approx("trip_distance", 0.5), 1).alias("median_miles"),
        F.round(F.percentile_approx(
            (F.unix_timestamp("dropoff_datetime_utc") - F.unix_timestamp("pickup_datetime_utc")) / 60.0, 0.5
        ), 1).alias("median_minutes")
    )
    .orderBy("fare_bucket")
    .show()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_green = df_taxi_green.filter(df_taxi_green["total_amount_usd"] <= 350)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_green.filter(df_taxi_green["trip_distance"] == 0).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_green = (
    df_taxi_green
    .withColumn(
        "duration_sec",
        F.unix_timestamp("dropoff_datetime_utc") - F.unix_timestamp("pickup_datetime_utc")
    )
    .filter(
        ~((F.col("trip_distance") == 0) & 
        ~F.coalesce((F.col("duration_sec") >= 60) & (F.col("total_amount_usd") >= 2.50), F.lit(False)))
        )
    .drop("duration_sec")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_green

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_green.write.format("delta").mode("append").saveAsTable("silver.taxi_trips")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi = spark.read.table("silver.taxi_trips")
df_taxi = df_taxi.filter(df_taxi["total_amount_usd"] <= 350)
df_taxi = (
    df_taxi
    .withColumn(
        "duration_sec",
        F.unix_timestamp("dropoff_datetime_utc") - F.unix_timestamp("pickup_datetime_utc")
    )
    .filter(
        ~((F.col("trip_distance") == 0) & 
        ~F.coalesce((F.col("duration_sec") >= 60) & (F.col("total_amount_usd") >= 2.50), F.lit(False)))
        )
    .drop("duration_sec")
)
df_taxi.write.format("delta").mode("overwrite").saveAsTable("silver.taxi_trips")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### For-Hire taxi trips

# CELL ********************

df_taxi_fhv = spark.read.table("bronze.fhv_taxi_trips")
df_taxi_fhv.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_fhv = df_taxi_fhv.select(
    F.to_utc_timestamp(F.col("pickup_datetime").cast("timestamp"), "America/New_York").alias("pickup_datetime_utc"),
    F.to_utc_timestamp(F.col("dropOff_datetime").cast("timestamp"), "America/New_York").alias("dropoff_datetime_utc"),
    F.col("PULocationID").cast("int").alias("pickup_location_id"),
    F.col("DOLocationID").cast('int').alias("dropoff_location_id"),
    F.lit(None).alias("total_amount_usd"),
    F.lit("fhv").alias("taxi_type")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_taxi_fhv.summary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_taxi_fhv.groupBy("pickup_location_id").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_fhv = df_taxi_fhv.withColumn("pickup_location_id", 
    F.when(F.col("pickup_location_id") == -1, F.lit(None))
    .otherwise(F.col("pickup_location_id"))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_taxi_fhv.groupBy("pickup_datetime_utc").count().orderBy("pickup_datetime_utc").limit(100))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_fhv = df_taxi_fhv.withColumn("dropoff_datetime_utc", 
    F.when(F.col("dropoff_datetime_utc") < F.lit("2015-01-01"), 
    F.lit(None))
    .otherwise(F.col("dropoff_datetime_utc"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_taxi_fhv.limit(30))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_fhv.write.format("delta").mode("append").saveAsTable("silver.taxi_trips")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transforming FHVHV

# CELL ********************

df_taxi_fhvhv = spark.read.table("bronze.fhvhv_taxi_trips")
df_taxi_fhvhv.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_fhvhv.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT COUNT(*) FROM silver.taxi_trips WHERE taxi_type = 'fhvhv'").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_fhvhv.groupBy("airport_fee").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_fhvhv = df_taxi_fhvhv.select(
    F.to_utc_timestamp(F.col("pickup_datetime").cast("timestamp"), "America/New_York").alias("pickup_datetime_utc"),
    F.to_utc_timestamp(F.col("dropoff_datetime").cast("timestamp"), "America/New_York").alias("dropoff_datetime_utc"),
    F.col("trip_miles").cast("double").alias("trip_distance"),
    F.col("PULocationID").cast("int").alias("pickup_location_id"),
    F.col("DOLocationID").cast('int').alias("dropoff_location_id"),
    F.col("base_passenger_fare").cast("double"),
    F.col("tolls").cast("double"),
    F.col("bcf").cast("double"),
    F.col("sales_tax").cast("double"),
    F.col("congestion_surcharge").cast("double"),
    F.col("cbd_congestion_fee").cast("float"),
    F.col("tips").cast("double"),
    F.col("airport_fee").cast("double"),
    F.lit(0).cast("double").alias("total_amount_usd"),
    F.lit("fhvhv").alias("taxi_type")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fee_columns = ["base_passenger_fare", "tolls", "bcf", "sales_tax", "congestion_surcharge", "tips", "airport_fee", "cbd_congestion_fee"]
display(df_taxi_fhvhv[fee_columns].summary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

total_amount_usd = F.round(
    sum(F.coalesce(F.col(c).cast("double"), F.lit(0.0)) for c in fee_columns),
    2
)

df_taxi_fhvhv = df_taxi_fhvhv.withColumn("total_amount_usd", total_amount_usd)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_fhvhv = df_taxi_fhvhv.select(
    F.col("pickup_datetime_utc"),
    F.col("dropoff_datetime_utc"),
    F.col("trip_distance"),
    F.col("pickup_location_id"),
    F.col("dropoff_location_id"),
    F.col("total_amount_usd"),
    F.col("taxi_type")
)
df_taxi_fhvhv.write.format("delta").mode("append").saveAsTable("silver.taxi_trips")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_taxi_fhvhv.summary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_fhvhv.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creating a sensor table with mapped zones

# CELL ********************

df_sensors = spark.read.table("silver.air_quality_measurements")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sensors[["sensor_id", "longitude", "latitude"]].distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sensors_locations = df_sensors[["sensor_id", "longitude", "latitude"]]

coords = df_sensors_locations.select(
    F.round(F.col("longitude").cast("double"), 4).alias("lon"),
    F.round(F.col("latitude").cast("double"), 4).alias("lat")
).distinct().toPandas()

def load_taxi_zones():
    return gpd.read_file("/lakehouse/default/Files/taxi_zones/taxi_zones.shp").to_crs("EPSG:4326")[["LocationID", "geometry"]]

def create_points(coords):
    return gpd.GeoDataFrame(
        coords,
        geometry=gpd.points_from_xy(coords["lon"], coords["lat"]),
        crs="EPSG:4326"
    )

def lookup_zones(coords):
    return (
        gpd.sjoin(create_points(coords), load_taxi_zones(), how="inner", predicate="within")[["lon", "lat", "LocationID"]]
        .drop_duplicates(subset=["lon", "lat"])
    )

lookup = lookup_zones(coords)
lookup_broadcast = F.broadcast(spark.createDataFrame(lookup))

mapped = (
    df_sensors_locations
    .withColumn("lon_r", F.round(F.col("longitude").cast("double"), 4))
    .withColumn("lat_r", F.round(F.col("latitude").cast("double"), 4))
    .join(lookup_broadcast, (F.col("lon_r") == F.col("lon")) & (F.col("lat_r") == F.col("lat")), "left")
    .withColumn("zone_id", F.col("LocationID").cast("int"))
    .drop("lon_r", "lat_r", "lon", "lat", "LocationID")
)

display(mapped)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mapped = mapped.withColumn("zone_id", F.when(F.col("zone_id").isNull(), 264).otherwise(F.col("zone_id")))
sensor_zone_lookup = F.broadcast(
    mapped.select("sensor_id", "zone_id").dropDuplicates(["sensor_id"])
)

df_sensors = (
    df_sensors.join(sensor_zone_lookup, on="sensor_id", how="left")
    .withColumn("zone_id", F.coalesce(F.col("zone_id")))
    .drop("longitude", "latitude", "location_id")
)
display(df_sensors)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sensors.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("silver.air_quality_measurements")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Transform zones to DimZones

# CELL ********************

taxi_zones:pd.DataFrame = gpd.read_file("/lakehouse/default/Files/taxi_zones/taxi_zones.shp").to_crs("EPSG:4326")[["LocationID", "geometry"]]
centroids = taxi_zones.to_crs("EPSG:2263").geometry.centroid.to_crs("EPSG:4326")
taxi_zones["longitude"] = centroids.x.round(6)
taxi_zones["latitude"] = centroids.y.round(6)
taxi_zones = taxi_zones.drop(columns=["geometry"])
zones_description = spark.read.table("bronze.taxi_zones").toPandas()
dim_zone = spark.createDataFrame(taxi_zones.merge(zones_description, how="right", on="LocationID"))
dim_zone.write.format("delta").mode("overwrite").saveAsTable("silver.DimZone")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi = spark.table("silver.taxi_trips")

df_duration_anomalies = (
    df_taxi
    .withColumn(
        "duration_hours",
        (F.unix_timestamp("dropoff_datetime_utc") - F.unix_timestamp("pickup_datetime_utc")) / 3600.0
    )
    .filter((F.col("duration_hours") < 0) | (F.col("duration_hours") > 24))
)

print(f"wrong durations rows: {df_duration_anomalies.count()}")
df_duration_anomalies.select(
    "taxi_type", "pickup_datetime_utc", "dropoff_datetime_utc", "duration_hours", "trip_distance", "total_amount_usd"
).show(10, truncate=False)

df_taxi_clean = (
    spark.table("silver.taxi_trips")
    .withColumn(
        "duration_sec",
        F.unix_timestamp("dropoff_datetime_utc") - F.unix_timestamp("pickup_datetime_utc")
    )
    .filter(
        F.col("pickup_datetime_utc").between("2015-01-01 00:00:00", "2026-12-31 23:59:59") &
        F.col("dropoff_datetime_utc").between("2015-01-01 00:00:00", "2027-01-01 23:59:59") &
        (F.col("duration_sec") >= 0) &
        (F.col("duration_sec") <= 24 * 60 * 60)
    )
    .drop("duration_sec")
)

(
    df_taxi_clean.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver.taxi_trips")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
