# API keys live in a Databricks secret scope, never in notebook source:
#   databricks secrets create-scope rental-pipeline
#   databricks secrets put-secret rental-pipeline rentcast-api-key
#   databricks secrets put-secret rental-pipeline anthropic-api-key

WILMINGTON_CITY = "Wilmington"
WILMINGTON_STATE = "NC"
DATABASE_NAME = "rental_pipeline"

# create db
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
spark.sql(f"USE {DATABASE_NAME}")

print("Config loaded.")
print(f"Database: {DATABASE_NAME}")
