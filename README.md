# SQL Data Warehouse Project

[![GitHub repo size](https://img.shields.io/github/repo-size/bhupalreddythandlam/sql-datawarehouse-project)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## 📌 Project Overview
This repository contains the complete codebase for a modern **SQL Data Warehouse**. The project leverages a robust **Medallion Data Architecture (Multi-hop)** to systematically ingest, clean, and model enterprise data, ensuring it is analytics-ready for business intelligence.

The data pipeline integrates two primary source systems:
* **ERP (Enterprise Resource Planning):** Consolidating internal company data, operational records, and backend logistics.
* **CRM (Customer Relationship Management):** Managing customer-facing interactions, sales pipelines, and user engagement metrics.

## 🏗️ Data Architecture (Medallion Concept)

The warehouse processes data in three distinct stages to ensure data quality, scalability, and clarity:

* **🥉 Bronze Layer (Raw Data):** Acts as the landing zone. Data is ingested from the CRM and ERP systems in its native format. We utilize custom T-SQL stored procedures (e.g., `bronze.load_bronze`) to perform bulk data loading efficiently without immediate transformations.
* **🥈 Silver Layer (Cleansed & Conformed):** The raw data undergoes filtering, data type casting, duplicate removal, and standardization. This layer acts as the single source of truth for the enterprise.
* **🥇 Gold Layer (Business-Ready):** Data is modeled into dedicated fact and dimension tables (Star/Snowflake schemas) optimized for high-performance querying and downstream analytics.

## 🛠️ Technology Stack
* **Database / SQL Dialect:** T-SQL
* **Core Architecture:** Medallion (Multi-hop) Data Architecture
* **Version Control:** Git & GitHub

## 📂 Repository Structure
*Module-wise code implementations make it simple to deploy individual pipeline stages.*

```text
sql-datawarehouse-project/
├── src/
│   ├── bronze/          # Stored procedures for raw data ingestion (e.g., load_bronze.sql)
│   ├── silver/          # Transformation scripts and data cleansing logic
│   └── gold/            # Fact and dimension table DDLs and aggregation scripts
├── docs/                # Architecture diagrams and schema documentation
└── README.md
