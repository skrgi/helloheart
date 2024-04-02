## Project Files
- dags
    - helloheart_dag.py - This file contains the Airflow DAG definition for the workflow. It specifies the tasks, their dependencies and the schedule
    - scripts/sql_statements.py - This script contains SQL statements and queries required for creating, inserting and generating the base and summary tables called by the tasks
    - config.py - To store db credentials
- Compose.yml - YAML file to orchestrate Docker containers
- Dockerfile - Contains the commands to create the custom Docker image
- requirements.txt - This file lists all Python packages and dependencies. These packages will be installed when building the Docker image
- .env - This file is used to store environment variables. These variables includes sensitive information like database credentials and configuration values.

## Workflow
Let's break this down into major steps

- Build Docker containers
- Run the DAG
	- Get input data from API
	- Load data to a Postgres DB
	- Analyze the data

## Project Setup
### Download the prerequisites and setup the environment
- Prerequisites: MacOS, Docker Desktop
- Open a terminal, clone the repo and `cd` to this directory

### Build Docker containers
- Start Docker Desktop
- We can build the Docker containers using this command on the Terminal:
    ```
    docker compose up -d
    ```

### Run and Monitor the DAG
- Open Airflow Web UI in a browser
    ```
    http://localhost:8080/
    ```
- Login
    ```
    username: airflow
    password: YOU_PASSWORD
    ```
- Turn on the DAG and Trigger Run
    ```
    healthdata_etl_dag
    ```
- Monitor the DAG execution and completion

### Summary
- Connect to the Postgres DB using psql on the Terminal
    ```
    docker exec -tiu postgres postgres psql
    ```
- Connect to the db
    ```
    \c helloheart
    ```
- Explore the aggregated tables
    ```
    SELECT * FROM covid_19.covid_test_results;
    SELECT * FROM covid_19.outcome_results;
    SELECT * FROM covid_19.smoothed_results;
    SELECT * FROM covid_19.state_results;
    SELECT * FROM covid_19.total_results;
    ```

## Observations
- This is a full-refresh. The DAG extracts the entire dataset from the API on each run and replaces the base table ```covid_19.covid_test_results```
- The primary key of the dataset is state + overall_outcome + date. There is no updated_date or inserted_date to perform an incremental load. There is an option to extract the data using only the date field with a URL formatting like:
    
    ```
    def max_date_query():
        sql_max_date = """
        SELECT to_char(max(date), 'YYYY-MM-DD') as max_date_str
        FROM covid_19.covid_test_results
        """
        return sql_max_date
    
    ..
    cur.execute(sql.SQL(max_date_query()))
    max_date = cur.fetchone()[0]

    url = f"""https://healthdata.gov/resource/j8mb-icvb.json?$where=date>'{max_date}T00:00:00'"""
    ```
- Although, the data availability doesn't allow for this. There are some states for which the data becomes readily available whereas for others the data might not become available till much later. Using a max_date filter might cause us to omit such records where date < max_date. Hence going with a full-refresh is the only way to ensure that all data between the source and destination is in sync for this dataset

# Architecture Overview

![Cloud Architecture](https://github.com/skrgi/helloheart/blob/main/cloud_architecture.jpg?raw=true)

### Amazon MWAA (Managed Workflows for Apache Airflow):
- Orchestrates the workflow of the data pipeline
- Executes tasks defined in the DAG

### Amazon S3 (Simple Storage Service):
- Stores the DAG script and any intermediate data
- Acts as a staging area for the data extracted from the API

### Amazon Glue:
- Provides the ETL engine for data processing
- Transforms the raw data fetched from the API into structured tables for analysis
- Integrates with the Glue Data Catalog for metadata management

### Amazon RDS (Relational Database Service):
- PostgreSQL RDS instance serves as the destination database
- Stores the transformed data ready for analysis and querying

### Amazon CloudWatch:
- Monitors the health and performance of the MWAA environment, Glue jobs and other services
- Provides logs and metrics for troubleshooting and monitoring

## Environment Setup

### 1. Setup AWS MWAA Environment
- Create an AWS MWAA environment with the provided configuration
- Configure Airflow plugins, requirements.txt file, secrets etc
- Create a S3 bucket for MWAA for DAG storage and logs

### 2. AWS Glue Job Setup
- Create a Glue job in the AWS Glue console
- Translate the script to Glue to extract from API, transform, load and aggregate
- Test and execute the Glue job to verify the data matches up correctly with the local setup

### 3. PostgreSQL RDS Instance Setup
- Create a new PostgreSQL RDS instance in the AWS RDS console
- Define the database schema and tables
- Note the database name, username, password and endpoint URL

### 4. Configure AWS Glue Connection in Airflow
- Create an AWS Connection in Airflow to allow interaction with AWS Glue

### 5. Configure PostgreSQL Connection in Airflow
- Create a PostgreSQL Connection in Airflow to connect to the RDS instance

### 6. Upload DAG Script to MWAA S3 Bucket
- Update the DAG script (`healthdata_etl_dag.py`) with AWSGlueJobOperator and upload to the MWAA S3 bucket

## Execution

### 1. Trigger DAG
- In the Airflow UI (accessible through the MWAA console), find the `healthdata_etl_dag` DAG
- Trigger the DAG manually or set up a schedule for automatic execution (`schedule_interval=timedelta(days=1)`)

### 2. Monitor Execution
- Monitor the progress of the DAG execution on the Airflow UI
- Check task logs for each operator to view status and logs

## Additional Considerations
- Configure logging settings in MWAA to store logs in S3
- Create IAM roles for MWAA, Glue, and RDS with appropriate permissions
- Use AWS IAM roles with least privilege principles
- Enable encryption for data at rest and in transit
- Configure network security using security groups and VPC Endpoints
- Secure Airflow UI with LDAP
- Configure auto-scaling for MWAA environments and Glue jobs
- Design Glue jobs for parallel processing and use partitioning techniques
- Monitor RDS performance and scale up instance sizes if needed
- Consider CeleryExecutor or KubernetesExecutor for Airflow task execution
- Right-size the MWAA environment, Glue job configurations and RDS instance
- Minimize data transfer costs and idle resource usage
- Set up CloudWatch alarms for monitoring and cost alerts
