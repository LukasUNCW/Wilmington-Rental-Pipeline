# Wilmington Rental Pipeline 

ETL pipeline built on Databricks that ingests live rental listings 
from the RentCast API, transforms them through a medallion architecture, and uses the 
Claude AI API to score and summarize each listing against our group preferences, 
helping three college students find the best house to rent in Wilmington, NC.

---

## Architecture

Built on the **medallion architecture** pattern common in production data engineering 
and mortgage/fintech data stacks.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Platform | Databricks (Serverless) |
| Storage | Delta Lake |
| Transformation | PySpark + SQL |
| AI Scoring | Anthropic Claude API (Haiku) |
| Listings Data | RentCast API |
| Orchestration | Databricks Jobs (daily schedule) |
| Visualization | Databricks SQL Dashboard |

---

## Pipeline Notebooks

| Notebook | Description |
|---|---|
| `config` | Project configuration and database setup |
| `bronze` | Calls RentCast API, lands raw JSON into Delta |
| `silver` | Cleans, deduplicates, normalizes, adds Maps URLs |
| `gold` | Batch scores listings via Claude API |

---

## How It Works

### Bronze Layer
Raw JSON responses from the RentCast API are landed as-is into a Delta table with 
an ingestion timestamp and source metadata. Append-only, nothing is ever modified 
or deleted at this layer.

### Silver Layer
The raw JSON is parsed into typed columns (bedrooms, bathrooms, price, coordinates, 
etc.), deduplicated by listing ID, and enriched with a Google Maps URL for each 
property. Records with null prices or bedroom counts are filtered out.

### Gold Layer
Each cleaned listing is passed to the Claude API with a structured prompt containing our
group's preferences (budget, bedroom count, property type, location). Claude 
returns a 1–10 score, per-person rent estimate, pros, cons, and a one-sentence 
summary for each listing. Results are joined back to the silver data and written 
as the final analytical asset.

### Dashboard
A Databricks SQL Dashboard surfaces the top listings (score ≥ 7) with a sortable 
table, monthly rent bar chart, and score distribution visualization.

---

## Key Results

- **50 listings** ingested and scored per run
- **23 listings** scored 7/10 or above
- **Top pick**: 3956 Echo Farms Blvd — 3bed/3bath for $1,749/month ($583/person)
- Pipeline runs automatically every day at 7:30 AM EST
- Email alerts sent daily to all three of us at 8:00 AM EST for any listing scoring 9/10

---

## Setup

### Prerequisites
- Databricks account (Community Edition works)
- RentCast API key — [sign up free](https://rentcast.io/api)
- Anthropic API key — [sign up here](https://console.anthropic.com) (may require the purchasing of Anthropic credits)

### Steps
1. Clone this repo into your Databricks Workspace
2. Open `config` and add your API keys
3. Run notebooks in order: `config` → `bronze` → `silver` → `gold`
4. Open the Databricks SQL Dashboard to view results
5. (Optional) Enable the Databricks Job for daily scheduling

---

## Skills Demonstrated

- Medallion architecture (Bronze / Silver / Gold)
- Delta Lake table management and schema evolution
- PySpark transformations and window functions
- REST API integration (RentCast + Anthropic)
- Agentic AI workflows — LLM as a scoring/reasoning layer in a data pipeline
- Databricks Jobs orchestration
- SQL Dashboard design

---

## Author

Lukas Nilsson — UNCW  
Built as a portfolio project for data engineering and AI integration roles
