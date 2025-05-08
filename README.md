# Superstore Sales Analysis

The Superstore Sales Analysis project is a comprehensive data analytics initiative that simulates an end-to-end ETL (Extract, Transform, Load) pipeline for retail sales data. The project focuses on ingesting, cleaning, and analyzing sales data from a fictional superstore, leveraging modern data engineering tools such as Docker, PostgreSQL, and Apache Airflow.

## Repository Outline
```
1. README.md - Project description
2. P2M3_ThalibanAllaam.txt - Contains the syntax (DDL and DML) used to insert the dataset into PostgreSQL inside the Docker container.
3. P2M3_ThalibanAllaam_data_raw.csv - Original dataset from the data source.
4. P2M3_ThalibanAllaam_data_clean.csv - Cleaned and processed data.
5. P2M3_ThalibanAllaam_DAG.py - Script used to run Apache Airflow with the following tasks:
    - Fetch data from PostgreSQL
    - Process data
    - Send the processed data to ElasticSearch
6. P2M3_ThalibanAllaam_DAG_graph.jpg - Graph of the DAG flow used in Apache Airflow.
7. P2M3_ThalibanAllaam_conceptual.txt - Answers to the conceptual questions.
8. P2M3_ThalibanAllaam_GX.ipynb - Notebook containing the data validation process of the cleaned data using Great Expectations.
9. /images - Folder containing images of the data visualization dashboard made using Kibana.
```

## Problem Background
`The competitive retail landscape demands data-driven strategies for success. The "Superstore Sales" dataset offers historical transaction data. Despite ongoing sales, a comprehensive understanding of key performance drivers is lacking. Critical questions regarding product profitability, regional performance variations, customer segment behavior, and temporal sales patterns remain largely unanswered. Analyzing this dataset is crucial to uncover actionable insights that will enable the superstore to make informed decisions, optimize operations, enhance customer satisfaction, and ultimately improve profitability in a competitive market.`

## Project Output
`The output of this project is a data visualization dashboard made using Kibana.`

## Data
`The dataset contains data regarding the overall sales performance of the Superstore. The dataset contains details regarding area performance, product performance, and customer segment behavior. The objective is to analyze the data to find any room for improvement that might help improve the Superstore's revenue.`

## Method
`With the help of Docker, we used Apache Airflow to automate the process. Starting from fetching the dataset in PostgreSQL, processing the data, and sending the data via ElasticSearch. The processed data is then used to create a data visualization dashboard using Kibana.`

## Stacks
Programming Language:
- Python

Tools:
- Docker
- PostgreSQL
- Apache Airflow
- ElasticSearch
- Kibana

---
