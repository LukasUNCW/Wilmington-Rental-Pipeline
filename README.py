# Databricks notebook source
# MAGIC %md
# MAGIC # Wilmington Rental Pipeline 
# MAGIC
# MAGIC A production-style ETL pipeline built on Databricks that ingests live rental listings from the RentCast API, transforms them through a medallion architecture, and uses the 
# MAGIC Claude AI API to score and summarize each listing against a group's preferences, 
# MAGIC helping three college students find the best house to rent in Wilmington, NC.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC Built on the **medallion architecture** pattern common in production data engineering 
# MAGIC and mortgage/fintech data stacks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Tech Stack
# MAGIC
# MAGIC | Layer | Technology |
# MAGIC |---|---|
# MAGIC | Data Platform | Databricks (Serverless) |
# MAGIC | Storage | Delta Lake |
# MAGIC | Transformation | PySpark + SQL |
# MAGIC | AI Scoring | Anthropic Claude API (Haiku) |
# MAGIC | Listings Data | RentCast API |
# MAGIC | Orchestration | Databricks Jobs (daily schedule) |
# MAGIC | Visualization | Databricks SQL Dashboard |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pipeline Notebooks
# MAGIC
# MAGIC | Notebook | Description |
# MAGIC |---|---|
# MAGIC | `00_config` | Project configuration and database setup |
# MAGIC | `01_bronze_ingest` | Calls RentCast API, lands raw JSON into Delta |
# MAGIC | `02_silver_transform` | Cleans, deduplicates, normalizes, adds Maps URLs |
# MAGIC | `03_gold_score` | Batch scores listings via Claude API |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How It Works
# MAGIC
# MAGIC ### Bronze Layer
# MAGIC Raw JSON responses from the RentCast API are landed as-is into a Delta table with 
# MAGIC an ingestion timestamp and source metadata. Append-only — nothing is ever modified 
# MAGIC or deleted at this layer.
# MAGIC
# MAGIC ### Silver Layer
# MAGIC The raw JSON is parsed into typed columns (bedrooms, bathrooms, price, coordinates, 
# MAGIC etc.), deduplicated by listing ID, and enriched with a Google Maps URL for each 
# MAGIC property. Records with null prices or bedroom counts are filtered out.
# MAGIC
# MAGIC ### Gold Layer
# MAGIC Each cleaned listing is passed to the Claude API with a structured prompt containing 
# MAGIC the group's preferences (budget, bedroom count, property type, location). Claude 
# MAGIC returns a 1–10 score, per-person rent estimate, pros, cons, and a one-sentence 
# MAGIC summary for each listing. Results are joined back to the silver data and written 
# MAGIC as the final analytical asset.
# MAGIC
# MAGIC ### Dashboard
# MAGIC A Databricks SQL Dashboard surfaces the top listings (score ≥ 7) with a sortable 
# MAGIC table, monthly rent bar chart, and score distribution visualization.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Results
# MAGIC
# MAGIC - **50 listings** ingested and scored per run
# MAGIC - **23 listings** scored 7/10 or above
# MAGIC - **Top pick**: 3956 Echo Farms Blvd — 3bed/3bath for $1,749/month ($583/person)
# MAGIC - Pipeline runs automatically every day at 6:00 AM ET
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Setup
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Databricks account (Community Edition works)
# MAGIC - RentCast API key — [sign up free](https://rentcast.io/api)
# MAGIC - Anthropic API key — [sign up here](https://console.anthropic.com)
# MAGIC
# MAGIC ### Steps
# MAGIC 1. Clone this repo into your Databricks Workspace
# MAGIC 2. Open `00_config` and add your API keys
# MAGIC 3. Run notebooks in order: `config` → `bronze` → `silver` → `gold`
# MAGIC 4. Open the Databricks SQL Dashboard to view results
# MAGIC 5. Optionally enable the Databricks Job for daily scheduling
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Skills Demonstrated
# MAGIC
# MAGIC - Medallion architecture (Bronze / Silver / Gold)
# MAGIC - Delta Lake table management and schema evolution
# MAGIC - PySpark transformations and window functions
# MAGIC - REST API integration (RentCast + Anthropic)
# MAGIC - Agentic AI workflows — LLM as a scoring/reasoning layer in a data pipeline
# MAGIC - Databricks Jobs orchestration
# MAGIC - SQL Dashboard design
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Author
# MAGIC
# MAGIC Lukas Nilsson — UNCW  
# MAGIC Built as a portfolio project for data engineering and AI integration roles

# COMMAND ----------

