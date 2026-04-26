# Databricks notebook source
DATABASE_NAME = "rental_pipeline"
RENTCAST_API_KEY = "your-rentcast-api-key-here"
ANTHROPIC_API_KEY = "your-anthropic-api-key-here"
spark.sql(f"USE {DATABASE_NAME}")
print("Ready.")

# COMMAND ----------

import requests
import json
import re

# Your group's preferences — edit these to match what you're actually looking for
PREFERENCES = """
- Budget: max $1,000/month per person, 3 people splitting rent = max $3,000/month total
- Bedrooms: at least 3
- Bathrooms: at least 2
- Minimum square footage: 1,200 sqft
- Property types: Single Family or Townhouse preferred, no studios
- Location: prefer to be within reasonable distance of UNCW (Wilmington, NC), say within 7 miles
- Dealbreakers: price over $5,000/month, less than 2 bathrooms
"""

def score_listing(listing_row):
    prompt = f"""You are helping a group of 3 college students find a house to rent in Wilmington, NC.

Their preferences:
{PREFERENCES}

Here is a rental listing:
- Address: {listing_row['address']}
- Property Type: {listing_row['property_type']}
- Bedrooms: {listing_row['bedrooms']}
- Bathrooms: {listing_row['bathrooms']}
- Square Footage: {listing_row['square_footage']}
- Monthly Rent: ${listing_row['monthly_rent']}
- Days on Market: {listing_row['days_on_market']}
- Zip Code: {listing_row['zip_code']}

Score this listing from 1-10 based on how well it matches their preferences.
Respond in this exact JSON format with no other text:
{{
  "score": <number 1-10>,
  "per_person_rent": <monthly_rent divided by 3, rounded to nearest dollar>,
  "pros": "<2-3 key positives, comma separated>",
  "cons": "<2-3 key concerns, comma separated>",
  "summary": "<one sentence recommendation>"
}}"""

    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 300,
            "messages": [{"role": "user", "content": prompt}]
        }
    )

    raw_text = response.json()["content"][0]["text"]
    
    # Strip markdown code fences if present
    clean = re.sub(r"```json|```", "", raw_text).strip()
    
    return json.loads(clean)

print("Scoring function defined.")

# COMMAND ----------

df_silver = spark.table("rental_pipeline.silver_listings")
sample = df_silver.limit(1).toPandas().iloc[0]

print(f"Testing on: {sample['address']}")
result = score_listing(sample)
print(json.dumps(result, indent=2))

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col

# Load all silver listings
df_silver = spark.table("rental_pipeline.silver_listings")
listings_pd = df_silver.toPandas()

print(f"Scoring {len(listings_pd)} listings...")

# Score each listing
results = []
for i, row in listings_pd.iterrows():
    try:
        score = score_listing(row)
        results.append({
            "listing_id": row["listing_id"],
            "score": score["score"],
            "per_person_rent": score["per_person_rent"],
            "pros": score["pros"],
            "cons": score["cons"],
            "summary": score["summary"]
        })
        print(f"  [{i+1}/{ len(listings_pd)}] {row['address']} → score: {score['score']}")
    except Exception as e:
        print(f"  [{i+1}/{len(listings_pd)}] FAILED: {row['address']} — {e}")

print(f"\nDone. Scored {len(results)} listings.")

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col

# Clean up per_person_rent — strip $ and convert to integer
results_clean = []
for r in results:
    r["per_person_rent"] = int(str(r["per_person_rent"]).replace("$", "").replace(",", "").strip())
    r["score"] = int(r["score"])
    results_clean.append(r)

# Convert to Spark DataFrame
df_scores = spark.createDataFrame(pd.DataFrame(results_clean))

# Join scores back to silver listings
df_gold = df_silver.join(df_scores, on="listing_id", how="inner")

# Write Gold table sorted by score descending
df_gold.orderBy(col("score").desc()) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("rental_pipeline.gold_listings")

print(f"Gold table written with {df_gold.count()} listings.")
