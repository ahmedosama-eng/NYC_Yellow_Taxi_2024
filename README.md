# AI-Powered Data Analysis Project

## Overview
This project implements an AI-driven data analysis pipeline that integrates Large Language Models (LLMs) to generate comprehensive insights. The system processes data through multiple stages, from ingestion to visualization, ensuring intelligent analysis and decision-making support.

## Pipeline Architecture
### 1. Data Reader Module
This module provides utilities for reading data files and lookup tables using PySpark.

#### Functions
##### `reading_the_files_in_df(spark)`
- Reads parquet files from a specified directory structure and returns them as a dictionary of Spark DataFrames.
- **Input**: Spark session object
- **Returns**: Dictionary where:
  - Keys: File names extracted from the file path
  - Values: Corresponding Spark DataFrames containing the parquet file data

##### `reading_the_lookups(spark)`
- Reads CSV lookup tables from a specified directory and returns them as a dictionary of Spark DataFrames.
- **Input**: Spark session object
- **Returns**: Dictionary where:
  - Keys: File names (without extension) from the lookup files
  - Values: Corresponding Spark DataFrames containing the CSV data

### 2. Transform Module
Handles data transformation operations for taxi trip data using PySpark, with specialized transformations for both LLM processing and Data Warehouse (DWH) loading.

#### Transform Functions
##### `Transform_Cleaning_and_Marge_LLM`
- Cleans and transforms data for LLM processing.
- Joins multiple lookup tables:
  - Taxi zone lookup (pickup and dropoff)
  - Rate lookup
  - Payment type lookup
  - Vendor lookup
  - Store and forward flag lookup
- **Generated Features**:
  - Time-based attributes (duration, month, day, hour, time of day classification)
- **Data Cleaning**:
  - Timestamp conversions
  - Null value handling for passenger count, congestion surcharge, airport fee, and other fields

##### `Transform_Cleaning_and_Marge_DWH`
- Transforms data specifically for DWH loading.
- **Features**:
  - Splits timestamps into date and time columns
  - Generates date keys (yyyyMMdd format)
  - Calculates trip duration
  - Categorizes time of day
- **Generated Dimensions**:
  - pickup_date_key, dropoff_date_key, duration, time of day classification

### 3. Data Warehouse Pipeline
#### A. Extract (`Extract/data_reader.py`)
- **Reads Raw Data**:
  - Reads taxi trip data from parquet files
  - Reads lookup tables from CSV files
  - Handles dynamic file paths and automatic file name parsing

#### B. Transform (`Transform/data_transformer.py`)
- **Fact Table Transformation**:
  - Timestamp processing
  - Date/time dimension creation
  - Duration calculations
  - Data type casting and null value handling
- **Dimension Tables**:
  - Store and forward flag
  - Payment type
  - Rate code
  - Taxi zone
  - Vendor
  - Date

#### C. Load (`load/data_loader.py`)
- **Database Loading**:
  - Loads transformed data into a PostgreSQL Data Warehouse
  - Uses JDBC connection handling
  - Implements overwrite mode for updates

### 4. Data Warehouse Schema
#### Fact Table (`taxi_facts`)
- **Metrics**: Trip distance, fare amount, extra charges, tips, total amount, duration
- **Foreign Keys**: pickup_date_key, dropoff_date_key, VendorID, RatecodeID, PULocationID, DOLocationID, payment_type

#### Dimension Tables
1. **Date Dimension (`date_dim`)**
   - Date key (yyyyMMdd), full date, year, month, day, day of week, time of day
2. **Location Dimension (`taxi_zone_dim`)**
   - LocationID, borough, zone, service zone
3. **Payment Dimension (`payment_type_dim`)**
   - Payment ID, payment type, description
4. **Rate Dimension (`rate_dim`)**
   - Rate code ID, rate type, description
5. **Vendor Dimension (`vendor_dim`)**
   - Vendor ID, vendor name, details

### 5. LLM Pipeline
#### A. Data Transformation (`Transform/data_transformer.py`)
- **Transform_Cleaning_and_Marge_LLM**
  - Joins lookup tables (taxi zones, rates, payment types)
  - Generates time-based features
  - Cleans data and handles null values
  - Creates enriched dataset for LLM processing

#### B. Data Ingestion (`LLM_integration/data_ingestion.py`)
- **prepare_for_embedding**
  - Creates six analytical views:
    - Vendor analysis
    - Drop-off location analysis
    - Pickup location analysis
    - Vendor impact studies
    - Comprehensive trip analysis
  - Generates aggregated pivot tables

#### C. Embedding Process (`LLM_integration/Embadding.py`)
- **Document Conversion**:
  - Converts DataFrames to document format
  - Processes in batches of 1000 rows
- **Text Chunking**:
  - Splits documents into 500-character chunks with 20-character overlap
- **Vector Database Creation**:
  - Uses OpenAI embeddings
  - Stores in FAISS vector database
  - Implements caching mechanism

#### D. Chatbot Integration (`LLM_integration/chatbot.py`)
- **Model Configuration**:
  - Uses GPT-3.5-turbo
  - Implements conversation memory
  - Creates retrieval chain for context
- **Query Processing**:
  - Maintains conversation history
  - Performs context-aware responses
  - Integrates with vector database

### 6. API Implementation (`LLM_pipline.py`)
- Flask-based REST API
- **Endpoint**: `/NYC_Taxi_2024`
- Handles POST requests for queries
- Returns AI-generated responses

