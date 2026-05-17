## Project Overview

This project demonstrates an end-to-end ETL pipeline built using PySpark. The pipeline ingests raw CSV data, performs cleaning and transformations, and stores optimized output in Parquet format.

## Architecture

Raw CSV
   ↓
Data Ingestion
   ↓
Schema Validation
   ↓
Data Cleaning
   ↓
Transformation
   ↓
Analytics Layer
   ↓
Parquet Storage

## Dataset Schema

order_id
customer_id
product
amount
city
date

## Features

- Explicit schema handling
- Missing value processing
- Invalid record filtering
- Data transformations
- Aggregation layer
- Parquet optimization
- Partitioning by city

## Technologies Used

- PySpark
- Databricks
- Parquet
- Python

## Project Flow

### Step 1: Data Ingestion
Read CSV using predefined schema.

### Step 2: Data Validation
- printSchema()
- describe()
- show()

### Step 3: Data Cleaning
- Remove null customer IDs
- Remove negative amounts
- Fill missing city values
- Remove null dates

### Step 4: Transformations
- Convert string to date
- Calculate tax
- Compute final amount

### Step 5: Analytics
Revenue by city

### Step 6: Storage
Store output in Parquet and partition by city.

## Sample Output

city=Bangalore  
city=Delhi  
city=Mumbai

## Skills Demonstrated

- ETL Design
- Data Engineering
- Schema Management
- PySpark Transformations
- Data Cleaning
- Partitioning
