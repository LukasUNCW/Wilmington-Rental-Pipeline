# Databricks notebook source
# Run config first to load variables
RENTCAST_API_KEY = "9a677050ebd4471aac1271d3ccf5ad38"  # same key from 00_config
DATABASE_NAME = "rental_pipeline"
spark.sql(f"USE {DATABASE_NAME}")

# COMMAND ----------

import requests
import json
from datetime import datetime

url = "https://api.rentcast.io/v1/listings/rental/long-term"

params = {
    "city": "Wilmington",
    "state": "NC",
    "limit": 50
}

headers = {
    "X-Api-Key": RENTCAST_API_KEY,
    "accept": "application/json"
}

response = requests.get(url, headers=headers, params=params)
print(f"Status: {response.status_code}")
print(f"Listings returned: {len(response.json())}")
print(json.dumps(response.json()[0], indent=2))  # preview first listing

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StringType
import json

# raw JSON to Spark DataFrame
raw_data = response.json()

# store each listing as a raw JSON string + metadata
rows = [(json.dumps(listing), listing["id"]) for listing in raw_data]

df_bronze = spark.createDataFrame(rows, ["raw_json", "listing_id"])

# ddd ingestion metadata
df_bronze = df_bronze \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("source", lit("rentcast_api")) \
    .withColumn("city", lit("Wilmington")) \
    .withColumn("state", lit("NC"))

# write to Delta table
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("rental_pipeline.bronze_listings")

print(f"Written {df_bronze.count()} records to bronze_listings")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     listing_id,
# MAGIC     ingested_at,
# MAGIC     source,
# MAGIC     raw_json
# MAGIC FROM rental_pipeline.bronze_listings
# MAGIC LIMIT 5