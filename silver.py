# Databricks notebook source
DATABASE_NAME = "rental_pipeline"
spark.sql(f"USE {DATABASE_NAME}")
print("Ready.")

# COMMAND ----------

from pyspark.sql.functions import col, from_json, to_timestamp, when
from pyspark.sql.types import *

# Define the schema based on what RentCast returns
listing_schema = StructType([
    StructField("id", StringType()),
    StructField("formattedAddress", StringType()),
    StructField("addressLine1", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("zipCode", StringType()),
    StructField("county", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("propertyType", StringType()),
    StructField("bedrooms", IntegerType()),
    StructField("bathrooms", DoubleType()),
    StructField("squareFootage", IntegerType()),
    StructField("price", DoubleType()),
    StructField("status", StringType()),
    StructField("listedDate", StringType()),
    StructField("removedDate", StringType()),
    StructField("daysOnMarket", IntegerType()),
])

# Read from bronze and parse JSON
df_bronze = spark.table("rental_pipeline.bronze_listings")

df_parsed = df_bronze \
    .withColumn("listing", from_json(col("raw_json"), listing_schema)) \
    .select(
        col("listing.id").alias("listing_id"),
        col("listing.formattedAddress").alias("address"),
        col("listing.zipCode").alias("zip_code"),
        col("listing.county").alias("county"),
        col("listing.latitude"),
        col("listing.longitude"),
        col("listing.propertyType").alias("property_type"),
        col("listing.bedrooms"),
        col("listing.bathrooms"),
        col("listing.squareFootage").alias("square_footage"),
        col("listing.price").alias("monthly_rent"),
        col("listing.status"),
        col("listing.daysOnMarket").alias("days_on_market"),
        col("listing.listedDate").alias("listed_date"),
        col("ingested_at")
    )

print(f"Parsed {df_parsed.count()} records")
df_parsed.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Deduplicate — keep the most recently ingested record per listing_id
window = Window.partitionBy("listing_id").orderBy(col("ingested_at").desc())

df_silver = df_parsed \
    .withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .filter(col("monthly_rent").isNotNull()) \
    .filter(col("bedrooms").isNotNull())

# Write to silver table
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("rental_pipeline.silver_listings")

print(f"Written {df_silver.count()} records to silver_listings")

# COMMAND ----------

from pyspark.sql.functions import concat, lit, regexp_replace, col

# Add Google Maps URL to silver table
df_silver_with_links = spark.table("rental_pipeline.silver_listings")

df_silver_with_links = df_silver_with_links.withColumn(
    "maps_url",
    concat(
        lit("https://www.google.com/maps/search/?api=1&query="),
        regexp_replace(col("address"), " ", "+")
    )
)

# Overwrite silver table with the new column
df_silver_with_links.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("rental_pipeline.silver_listings")

print("Maps URLs added to silver_listings.")