
# Parquet Ingestion and SQLite Data Pipeline

## Project Overview
This project builds a data engineering pipeline for air quality data using Python, Pandas, Parquet, and SQLite.

The workflow includes:
- Reading parquet datasets
- Data cleaning and transformation
- Building a SQLite database
- Designing a star schema data model
- Running analytical SQL queries

## Project Structure

parquet-ingestion-pipeline/
│
├── data/
│   ├── team_5 (3).parquet
│   └── database/
│       └── air_quality.db
│
├── notebooks/
│   ├── Team5_Parquet_Ingestion_Pipeline.ipynb
│   └── Team5_Data_Model_Milestone2.ipynb
│
├── scripts/
│   └── data_model.py
│
├── logs/
├── config/
└── README.md

## Technologies Used
- Python
- Pandas
- PyArrow
- SQLite
- Jupyter Notebook

## Database Model
The project uses a star schema model with:
- Dimension tables
- Fact tables
- SQL-based analytical queries

## Workflow
Parquet Data → Python ETL → SQLite Database → SQL Analysis
