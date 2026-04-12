🚀 Real-Time Crime Data Pipeline (Scala + Spark + Java)
📌 Overview

This project implements a real-time ETL data pipeline for processing crime data from Los Angeles.

Unlike traditional batch pipelines, this system includes a custom Java socket-based data source that streams crime records in real time.

The pipeline processes data using Apache Spark (Scala), stores it in PostgreSQL, and visualizes insights using Metabase.

🏗️ Architecture

The system follows a real-time ETL architecture:

Java Socket Source → Spark ETL Pipeline → PostgreSQL → Metabase Dashboard

⚙️ Data Flow
1. 📡 Data Ingestion (Java Source)
Custom Java application simulates a streaming data source
Sends Los Angeles crime records via TCP sockets
Each record is transmitted line-by-line in real time
2. 🧹 Data Cleaning (Spark)
Handle missing or invalid values
Standardize formats (dates, strings, categories)
Remove corrupted records
3. 🔄 Data Transformation (Spark)
Feature engineering (e.g., time-based attributes)
Aggregations (crime counts, trends)
Data enrichment and normalization
4. 💾 Data Storage (PostgreSQL)
Cleaned and transformed data stored in PostgreSQL
Structured schema for analytics queries
Optimized for dashboard consumption
5. 📊 Visualization (Metabase)
Interactive dashboards
Crime trend analysis
Geographic and temporal insights
KPI monitoring (crime frequency, patterns, etc.)
🛠️ Technologies Used
☕ Java (Socket-based data generator)
🔵 Apache Spark (Scala)
🐘 PostgreSQL
📊 Metabase
⚡ TCP Sockets (custom streaming layer)
🔥 Key Features
📡 Real-time data ingestion using Java sockets
🔄 ETL pipeline built with Apache Spark
🧹 Robust data cleaning and preprocessing
💾 Structured storage in PostgreSQL
📊 Interactive analytics dashboards
📊 Example Analyses
Most frequent crime types
Crime distribution by time of day
Hotspots of criminal activity
Temporal trends (daily / weekly patterns)
High-risk zones in Los Angeles
