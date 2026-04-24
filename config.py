# Databricks notebook source
# Project config
RENTCAST_API_KEY = "your_rentcast_api_key_here"

WILMINGTON_CITY = "Wilmington"
WILMINGTON_STATE = "NC"
DATABASE_NAME = "rental_pipeline"

# Create the database
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
spark.sql(f"USE {DATABASE_NAME}")

print("Config loaded.")
print(f"Database: {DATABASE_NAME}")