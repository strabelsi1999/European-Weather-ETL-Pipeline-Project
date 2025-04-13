# European-Weather-ETL-Pipeline-Project


## Project Summary

This project implements a fully automated ETL pipeline that collects real-time weather data from the OpenWeatherMap API for major European and North African cities. Using Apache Airflow, the pipeline ensures daily scheduled extraction, transformation, and loading of weather data into a Microsoft SQL Server database, enabling seamless reporting and analytics.
Tools & Technologies

- Python – ETL logic
- Apache Airflow – Workflow orchestration
- SQL Server Express – Relational database
- Pandas – Data transformation
- SQLAlchemy – Database connectivity
- OpenWeatherMap API – Real-time weather source

## Project Objectives

- Automate weather data collection for over 80 cities.
- Maintain a historical record of daily weather conditions.
- Store structured data in SQL Server for querying and analysis.
- Enable connection to BI tools (e.g., Power BI, Tableau) for visualization.

## Pipeline Overview
### Step 1: Orchestration with Apache Airflow

- Defined an Airflow DAG named weather_etl_pipeline.
- Configured to run daily at midnight (@daily).
- Each DAG run triggers a function to fetch and process weather data for multiple cities.

### Step 2: Extract Phase

- Calls the OpenWeatherMap API using Python requests.
- Retrieves temperature, weather condition, wind speed, humidity, and more.
- Handles HTTP errors and missing data gracefully.

### Step 3: Transform Phase

- Normalizes the JSON API response using pandas.
- Converts timestamps to readable datetime format.
- Prepares clean data suitable for SQL ingestion.

### Step 4: Load Phase

- Connects to SQL Server using SQLAlchemy.
- Loads data into the weather_data table.
- Configured with if_exists='append' to retain historical records.

## Geographic Coverage

### The pipeline targets a broad range of cities across:

- Western Europe (e.g., London, Paris, Madrid)
- Central & Eastern Europe (e.g., Vienna, Warsaw)
- Southern Europe & North Africa (e.g., Rome, Tunis, Casablanca, Cairo)

## Use Cases

- Generate interactive dashboards with historical weather trends.
- Analyze weather patterns for supply chain or travel planning.
- Power a recommendation engine based on live climate conditions.

## Conclusion

This project successfully demonstrates the implementation of a modern ETL pipeline using Python, SQL Server, and Airflow. By leveraging API data extraction from OpenWeatherMap, transforming it into a structured format, and storing it in a relational database, the pipeline ensures continuous and automated data flow. Integrating Airflow provided clear orchestration, scheduling, and monitoring of the workflow, making it scalable and production-ready.
