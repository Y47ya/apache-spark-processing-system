<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/4e08f43a-7e99-4c46-9887-0c5325be5e55" />



````markdown
# Real-Time Crime Data Pipeline

## Overview

This project implements a real-time ETL data pipeline for processing crime data from Los Angeles.

The system uses a custom Java socket-based data source to stream crime records in real time. The streamed data is consumed and processed by an Apache Spark pipeline written in Scala. After cleaning and transformation, the processed data is stored in PostgreSQL and visualized through Metabase dashboards.

The purpose of this project is to demonstrate a complete real-time data engineering workflow, starting from data ingestion and ending with analytical visualization.

## Architecture

The pipeline follows a real-time ETL architecture:

```text
Java Socket Source -> Apache Spark ETL Pipeline -> PostgreSQL -> Metabase Dashboard
````

## Architecture Diagram

The system is composed of four main stages:

```text
+----------------------+        +--------------------------+        +----------------------+        +----------------------+
| Java Socket Source   | -----> | Apache Spark ETL Pipeline | -----> | PostgreSQL Database  | -----> | Metabase Dashboard   |
|                      |        |                          |        |                      |        |                      |
| Data producer        |        | Cleaning                 |        | Structured storage   |        | Visual analytics     |
| TCP socket stream    |        | Transformation           |        | SQL queries          |        | Dashboards and KPIs  |
+----------------------+        +--------------------------+        +----------------------+        +----------------------+
```

## System Components

### Java Socket Source

A custom Java application acts as the real-time data producer.

It reads Los Angeles crime records and sends them line by line through TCP sockets. This simulates a streaming data source and allows Spark to process incoming records continuously.

Repository:

```text
https://github.com/Y47ya/data-streaming-system
```

Main responsibilities:

* Simulate a real-time data source
* Read crime records
* Send records through TCP sockets
* Stream data line by line

### Apache Spark ETL Pipeline

Apache Spark is used as the main processing engine.

The pipeline is written in Scala and is responsible for receiving streamed records, cleaning the data, transforming it, and preparing it for analytical storage.

Main responsibilities:

* Read streaming data from the TCP socket source
* Parse incoming crime records
* Handle missing, invalid, or corrupted values
* Standardize date, text, and categorical fields
* Create analytical features
* Apply transformations and aggregations
* Prepare the final dataset for PostgreSQL

### PostgreSQL Storage

PostgreSQL is used as the structured storage layer.

The cleaned and transformed records are stored in relational tables, making the data easy to query and suitable for analytics.

Main responsibilities:

* Store processed crime records
* Provide a structured schema for analysis
* Support SQL queries for reporting
* Serve as the data source for Metabase

### Metabase Dashboard

Metabase is used for data visualization and business intelligence.

It connects to PostgreSQL and provides interactive dashboards for exploring crime patterns and trends.

Main responsibilities:

* Visualize crime trends
* Display key performance indicators
* Support interactive analysis
* Provide charts and dashboards for decision-making

## Data Flow

### 1. Data Ingestion

The Java socket application streams Los Angeles crime records through a TCP connection.

Each record is transmitted line by line, allowing the pipeline to process data continuously instead of relying only on static batch files.

### 2. Data Cleaning

Spark processes the incoming records and applies data quality checks.

Typical cleaning operations include:

* Removing corrupted records
* Handling missing values
* Standardizing date formats
* Normalizing string fields
* Validating required columns
* Filtering unusable records

### 3. Data Transformation

After cleaning, Spark transforms the records into an analytics-ready format.

Typical transformations include:

* Extracting time-based attributes
* Creating crime category summaries
* Aggregating crime counts
* Preparing data for dashboard queries
* Normalizing fields for consistent analysis

### 4. Data Storage

The transformed data is written into PostgreSQL.

This layer stores the final processed data in structured tables that can be queried by SQL tools and BI dashboards.

### 5. Data Visualization

Metabase connects to PostgreSQL and provides dashboards for analyzing the processed crime data.

The dashboards make it possible to monitor crime trends, identify patterns, and explore insights interactively.

## Technologies Used

| Technology   | Role                                                     |
| ------------ | -------------------------------------------------------- |
| Java         | Custom socket-based streaming data producer              |
| TCP Sockets  | Real-time communication layer between producer and Spark |
| Apache Spark | Distributed ETL and stream processing engine             |
| Scala        | Main language used for the Spark pipeline                |
| PostgreSQL   | Relational database used for processed data storage      |
| Metabase     | Dashboarding and data visualization tool                 |

## Key Features

* Real-time data ingestion using a custom Java socket source
* Stream processing with Apache Spark and Scala
* Data cleaning and preprocessing
* Data transformation and feature engineering
* Structured storage in PostgreSQL
* Interactive dashboards with Metabase
* End-to-end real-time data engineering architecture

## Example Analyses

The processed data can be used to answer questions such as:

* What are the most frequent crime types?
* How does crime activity change by time of day?
* Which areas have the highest crime frequency?
* What are the daily or weekly crime trends?
* Which crime categories are increasing or decreasing over time?
* What are the main crime hotspots in Los Angeles?

## Project Structure

A typical project structure may look like this:

```text
.
├── src/
│   ├── main/
│   │   └── scala/
│   │       └── spark/
│   │           └── CrimeDataPipeline.scala
├── config/
│   └── application.conf
├── sql/
│   └── schema.sql
├── dashboards/
│   └── metabase/
├── README.md
└── build.sbt
```

The exact structure may vary depending on the implementation.

## How the Pipeline Works

1. The Java socket producer starts and opens a TCP socket.
2. Crime records are streamed line by line.
3. Spark connects to the socket source and reads incoming records.
4. Spark parses, cleans, and transforms the data.
5. The final processed data is written to PostgreSQL.
6. Metabase reads from PostgreSQL and displays interactive dashboards.

## Expected Output

At the end of the pipeline, the system provides:

* A cleaned and structured crime dataset in PostgreSQL
* Aggregated data ready for analysis
* Metabase dashboards for visual exploration
* Insights about crime frequency, trends, and patterns

## Purpose of the Project

This project is designed to demonstrate practical data engineering skills, including:

* Real-time data ingestion
* Stream processing
* ETL design
* Data cleaning
* Data transformation
* Relational data storage
* Dashboard creation
* End-to-end pipeline architecture

```
```
