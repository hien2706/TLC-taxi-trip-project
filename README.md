# ETL-piplines-using-Airflow

## Project Overview
-This is an ETL pipeline that takes data from websites and CSV files, transforms them using Python, and finally loads them into the Postgres database in the form of star schema.\
-Input: TLC trip data records, tables of zones' names and geometry, table of company affiliations' plate numbers.\
-Output: A DB with 4 tables(3 dimensions, 1 fact) ready for analysis.

## Technologies and skills
- Workflow orchestration: Apache Airflow (DAGs, Monitoring & Troubleshooting)
- Programming language: Python(Pandas,Requests)
- Database: PostgreSQL
- Data Modeling: Star Schema(4 steps to create dimensional model in data warehouse)
- Concatenation: Docker(docker-compose)

## Project workflow

![etl_workflow](https://github.com/hien2706/ETL-piplines-with-Airflow/blob/main/images/etl_pipeline.jpg)

## Data model

![star schema](https://github.com/hien2706/ETL-piplines-with-Airflow/blob/main/images/hvfhs.jpeg)

## Project steps
The project implements DAGs in Airflow to orchestrate the pipeline.
- First, data of TLC trips are downloaded, tables of zones and geometry are crawled from the web, and the table of Company Affiliation is hand-made using the information in the trips record user guide: https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf
- Second, we will resolve missing values, correct data types of columns, and drop duplicate rows. 
- After that, we will create dimension tables, as shown above, create surrogate keys for each table, and the fact table.
- Finally, we create tables and Foreign Key constraints in Postgres DB, and load the transformed data into the DB.

## Project graph in Airflow

![airflow graph](https://github.com/hien2706/ETL-piplines-with-Airflow/blob/main/images/airflow_UI.jpg)


