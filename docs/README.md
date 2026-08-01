# ✈️ Intelligent Aviation Reliability & Delay Prediction Platform

This project focuses on building a complete Big Data solution for analyzing historical flight operations and predicting flight delays. Using six years of US domestic flight data (2020–2025), the system processes large volumes of data through an automated Lakehouse pipeline, generates business-ready datasets, predicts arrival delays, and provides reliability insights for airlines, airports, and flight routes.

---

## Project Overview

The goal of this project is to convert raw flight data into meaningful business insights through an automated data engineering pipeline. Along with data processing, the project applies machine learning to predict flight delays and creates interactive dashboards for airline reliability and operational analysis.

The solution follows a Bronze–Silver–Gold architecture, making the data easy to maintain, scalable, and ready for analytics as new data becomes available.

---

## Objectives

- Process large-scale historical flight data efficiently.
- Build an automated data pipeline using a Lakehouse architecture.
- Predict whether a flight will arrive more than 15 minutes late.
- Measure the reliability of airlines, airports, and routes.
- Create dashboard-ready datasets for business reporting.
- Reduce manual effort through automated workflows.

---

## Technology Stack

### Cloud Services

- AWS S3
- AWS EC2
- AWS Glue
- Amazon Athena

### Big Data

- Apache Spark
- PySpark

### Programming

- Python

### Machine Learning

- Spark MLlib
- Ensemble Learning Models

### Visualization

- Power BI

### Automation

- GitHub Actions

### Infrastructure

- Terraform

---

## Dataset

- **Source:** Kaggle – US Domestic Flight Dataset
- **Duration:** 2020 to 2025
- **Records:** More than 40 million flight records
- **Storage Formats:** CSV and Parquet

---

# Project Workflow

```
Kaggle Flight Dataset
          │
          ▼
    Data Ingestion
          │
          ▼
    Bronze Layer (Raw CSV)
          │
          ▼
 Bronze → Silver ETL
 • Data Cleaning
 • Standardization
 • Validation
 • CSV to Parquet
 • Partitioning
          │
          ▼
    Silver Layer
          │
          ▼
Silver → Gold ETL
 • Feature Engineering
 • Reliability Scoring
 • Star Schema Generation
 • ML Dataset Creation
 • Dashboard Aggregations
          │
 ┌────────┴────────┐
 ▼                 ▼
Visualization     ML Dataset
Tables            (ArrDel15)
 │                 │
 ▼                 ▼
Power BI      Ensemble Learning
Dashboard          │
                   ▼
          Streamlit Application

```

---

# Bronze Layer

The Bronze layer stores the raw monthly CSV files exactly as received from the source. No transformations are applied at this stage so the original data is always preserved.

---

# Silver Layer

The Silver layer performs all preprocessing tasks required before analytics.

Main operations include:

- Data cleaning
- Removing duplicate records
- Selecting the required columns (35/120)
- Handling missing values
- Standardizing column values
- Converting CSV files to Parquet
- Partitioning data by Year

This layer contains cleaned and optimized data that is ready for business transformations.

---

# Gold Layer

The Gold layer contains business-ready datasets created from the Silver layer.

It consists of three different outputs.

### 1. Star Schema

The analytical model includes:

- Fact_Flights
- Dim_Date
- Dim_Airline
- Dim_Airport
- Dim_Route

These tables are mainly used for business analysis and reporting.

---

### 2. Machine Learning Dataset

A separate dataset is created specifically for model training.

Target Variable

```
ArrDel15

0 → Arrival delay is 15 minutes or less

1 → Arrival delay is greater than 15 minutes
```

The dataset contains engineered features that improve model performance while avoiding unnecessary columns used only for reporting.

---

### 3. Visualization Tables

Additional tables are generated specifically for dashboards.

Current visualization datasets include:

- Viz_Delay_Analytics
- Viz_Reliability_Analytics

These tables reduce dashboard loading time because most calculations are already completed during the ETL process.

---

# Machine Learning

The processed Gold dataset is used to train an Ensemble Learning model for predicting arrival delays.

The model learns from historical flight information, airline performance, airport statistics, route characteristics, and engineered features to predict whether a flight will be delayed by more than 15 minutes.

---

# Dashboard

Power BI dashboards provide insights such as:

- Airline Reliability
- Airport Performance
- Route Analysis
- Delay Distribution
- Seasonal Trends
- Top Performing Airlines

These dashboards help users understand operational performance through interactive visualizations.

---

# Automation

To reduce manual work, several parts of the project are automated.

Automation includes:

- Data ingestion
- ETL execution using AWS Glue
- GitHub Actions workflow
- Scheduled pipeline execution
- Infrastructure provisioning using Terraform

This makes the project easier to maintain and allows new data to be processed with minimal effort.

---

# Project Highlights

- Automated Bronze–Silver–Gold Lakehouse Pipeline
- Processes over 40 million flight records
- Distributed data processing using Apache Spark
- Feature engineering for machine learning
- Airline, Airport and Route Reliability Analysis
- Flight Delay Prediction using Ensemble Learning
- Dashboard-ready aggregated datasets
- Interactive Power BI dashboards
- Automated cloud-based data pipeline

---

# Project Repository

https://github.com/Group-3-BDA/flight-delay-analysis

---

**Developed as a PG-Diploma Major Project (GROUP - 3 ,PG-DBDA, CDAC).**
