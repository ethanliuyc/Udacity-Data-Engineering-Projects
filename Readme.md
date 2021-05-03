# Udacity Date Engineering Projects

## Project 1: Data Modeling with Postgres

This projects aims to create a Postgres database schema and a Python-based ETL pipeline. allowing the data to be queried and analyzed easily. A startup  wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The current data resides in a direcotry of JSON logs on user activity and dda directory with JSON metadata on songs in their app.

## Project 2: Data Modeling with Postgres

This projects aims to create a Cassandra database schema and a Python-based ETL pipeline. In this project, we need to 

- Model data by creating tables in Apache Cassandra based on frequently used queries (e.g. all user names in the app history who listened to a specific song)

- Process a set of CSV files to create the data file that will be used for Apache Casssandra tables

- Insert data into Apache Cassandra tables.

## Project 3: Data Warehouse with Redshift

This project aims to create a Data Warehouse on AWS Redshift, and build an ETL pipeline to extract and transform data stored in JSOPN format in S3 buckets and move the data to Warehouse hosted on Redshift. In this project, we need to:

- Create Data Warehouse on AWS Redshift and storage service on S3

- Create staging tables and STAR schema fact/dimension analytics tables on AWS Redshift

- Process data from staging tables into analytics tables on Redshift

## Project 4: Data Lake with Spark and EMR

This project aims to build a Data Lake on AWS cloud using Spark and AWS EMR cluster. In this project, we need to: 

- Write Spark scripts to to load data from S3, process the data into analytics tables

- Deploy this Spark process on a cluster using AWS EMR

## Project 5: Data Pipeline using Apache Airflow

This project aims to create a Data Pipeline using Apache Airflow to monitor and schedule ETL jobs. In this project, we need to:

- Design custom operators to stage the data, transform the data, and run checks on data quality. These operators need to be flexible, reusable, and configurable.
- Design DAG file to state the process and order of ETL workflow.
- Set up Redshift and AWS connectons on Airflow UI


