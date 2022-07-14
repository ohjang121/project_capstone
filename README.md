# Capstone Project: U.S. Immigration

Objective: Combine all data engineering concepts from the Udacity Data Engineering Nanodegree Program to design and build data models from unstructured, raw datasets.

### Project Summary
In this project, I integrate immigration, temperature, and demographics data to design and build data models for analytics purpose. I chose this recommended project with my strong interest in immigration as an immigrant myself.

Below are the recommended steps to tackle the project:

* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

---

### Step 1: Scope the Project and Gather Data

#### Data sources

1. [I94 Immigration Data](https://travel.trade.gov/research/reports/i94/historical/2016.html): I94 immigration data from Apr 2016. Contains ~3M rows. Stored in parquet.

2. [World Temperature Data](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data): World temperature data per city and country. Contains ~1M rows. Stored in csv.

3. [U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/): Demographics data per city and state in the U.S. Contains thousands of rows. Stored in csv.

4. [I94 Immigration Data Descriptions](https://github.com/ohjang121/project_capstone/blob/main/I94_SAS_Labels_Descriptions.SAS): This SAS file contains the data dictionary and mapping of alphanumeric codes to country / port. I extracted this data into 2 separate csvs - `i94citres_country_mapping.csv` and `i94port_city_state_mapping.csv` - which are stored in my public S3 bucket `udacity-capstone-joh/staging`.

With 2 out of the 4 data sources exceeding 1M rows of data and have 2 different file types (parquet, csv), they meet the data requirement for the project.

#### End Use Case

End use case for this project will be analytics - users will be able to use the data models to analyze how temperature and demographics for a given region may affect the immigration trend.

---

### Step 2: Explore and Assess the Data

Before exploring the raw datasets, I uploaded them to my public S3 bucket - `udacity-capstone-joh/staging`. This enables any user to explore the raw datasets easily without having to clone this repo or download the datasets locally. 

During the raw dataset uploading step, I extracted mapping of alphanumeric codes to country / port from [I94 Immigration Data Descriptions](https://github.com/ohjang121/project_capstone/blob/main/I94_SAS_Labels_Descriptions.SAS) into 2 separate csvs - `i94citres_country_mapping.csv` and `i94port_city_state_mapping.csv`. These mapping data are crucial to translate codified location columns in the immigration data.

[load_prod_data.py](https://github.com/ohjang121/project_capstone/blob/main/dags/load_prod_data.py) contains 4 spark sql queries that transform the raw datasets into more meaningful data with correct data type formatting. They also use the country / port mapping tables to get corresponding location values in the immigration data. Using those queries as inputs, [immigration_spark_etl.py](https://github.com/ohjang121/project_capstone/blob/main/dags/immigration_spark_etl.py) cleans and drops missing or wrong values with detailed documentation for each step (e.g. drop any rows that do not have gender = male or female). Finally, it adds surrogate keys using `row_number()` function in 2 dimensional tables that do not have primary keys.

---

### Step 3: Define the Data Model

#### Design Data Model
Because the end use case is for analytics that require frequent joins and aggregations, we will model the datasets in a star schema. As there are not that many datasets anyways, there is barely any risk in high data redundancy or lack of data integrity.

![alt text](https://github.com/ohjang121/project_capstone/blob/main/immigration_erd.png)

Data dictionary for the 4 tables are documented in [immigration_data_dict.yaml](https://github.com/ohjang121/project_capstone/blob/main/immigration_data_dict.yaml).

---

### Step 4: Run ETL to Model the Data 

#### Design Data Pipeline

![alt text](https://github.com/ohjang121/project_capstone/blob/main/immigration_dag_big.png)

Immigration DAG is set up via [immigration_dag.py](https://github.com/ohjang121/project_capstone/blob/main/dags/immigration_dag.py). For each task:

1. `Spark_ETL`: Runs [immigration_spark_etl.py](https://github.com/ohjang121/project_capstone/blob/main/dags/immigration_spark_etl.py) that performs ETL using the raw datasets stored in `udacity-capstone-joh/staging` S3 bucket to `udacity-capstone-joh/production` S3 bucket. All transformations are done in Spark, no need for additional transformation in Redshift.
    * Note: If [immigration_spark_etl.py](https://github.com/ohjang121/project_capstone/blob/main/dags/immigration_spark_etl.py) fails at dataframe read step with errors such as `java.lang.NumberFormatException: For input string: "64M"` or other unknown Java errors, it means your environment is not set up correctly with the SparkSession's config file. If you encounter this issue, you need to set up an EMR cluster with pre-built Spark configurations to avoid errors. Once you have the EMR cluster set up, scp [immigration_spark_etl.py](https://github.com/ohjang121/project_capstone/blob/main/dags/immigration_spark_etl.py) with `dl_cfg` file and [load_prod_data.py](https://github.com/ohjang121/project_capstone/blob/main/dags/load_prod_data.py) file to the EMR cluster and run `spark-submit immigration_spark_etl.py`. Once you confirm output parquet files are written in the `udacity-capstone-joh/production` S3 bucket with appropriate file size per table, you can safely mark this task `Success` and run the remaining downstream tasks. 

2. `AWS_redshift_setup`: Runs [aws_setup](https://github.com/ohjang121/project_capstone/blob/main/dags/aws_setup.py) to create an IAM role with Admin user access and a Redshift cluster. It logs the cluster endpoint and arn for the new Redshift cluster. If a cluster with same configurations is already running, it quickly finishes with a log indicating an existing cluster.

3. `Create_tables`: Runs 4 queries in [imm_create_tables.sql](https://github.com/ohjang121/project_capstone/blob/main/dags/imm_create_tables.sql) in the Redshift cluster set up in Step 2. It creates 1 fact table and 3 dimensional tables with primary keys.
    * Note: If this task fails due to host / connection issue to the redshift cluster, you must update the connection `redshift_capstone` in the Airflow UI: change `host` with the new cluster endpoint outputted in the `AWS_redshift_setup` task's log in Step 2. Unfortunately there is no API functionality that allows updating an existing Airflow connection's components based on the Redshift set up task. Potential improvement is to use Xcom, but for now it is recommended to update the Airflow connection in the UI and rerun this task using the up-to-date Redshift cluster.

4. `Load_table_to_redshift`: Runs [pq_stage_redshift.py](https://github.com/ohjang121/project_capstone/blob/main/plugins/operators/pq_stage_redshift.py) operator and copies the production data written in parquet into each table. All loads are formatted as parquet. As mentioned in Step 1, there is no need to do additional transformation to load data into the production tables. This is to allow `schema-on-read` to have identical output as querying in the Redshift database.

5. `Run_data_quality_checks`: Runs [data_quality.py](https://github.com/ohjang121/project_capstone/blob/main/plugins/operators/data_quality.py) operator that achieves 2 things per table: row_count > 0 & primary key checks. These basic checks will ensure that the tables are loaded correctly without any errors. It also logs the number of rows per table.

6. `AWS_redshift_terminate`: Runs [aws_setup](https://github.com/ohjang121/project_capstone/blob/main/dags/aws_setup.py) but with the argument `--delete`. This deletes the IAM role and Redshift cluster created for the data processing.

---

### Step 5: Complete Project Write Up

#### Tools & Technologies

1. **Spark**: I used PySpark to explore and assess the raw datasets. 2 reasons are 1) 2 of the datasets had few millions rows of data, which require decent computing power for fast querying, and 2) `schema-on-read` that enables SQL queries to transform the datasets as if they were already loaded in a database. [immigration_spark_etl.py](https://github.com/ohjang121/project_capstone/blob/main/dags/immigration_spark_etl.py) incorporates Spark in it.

2. **AWS Redshift**: I used Redshift to build the data models and load transformed data through Spark. End use case is for analytics, and creating a data warehouse to store production data in tables would allow the target audience of analysts to access the production data more easily. Among many relational dbs, Redshift was chosen due to its strength in analytical workloads and ability to parallelize execution of one query on multiple nodes as tables are partitioned across many nodes. Redshift usage would not be necessary if I force the users to strictly rely on `schema-on-read` and load production data from S3 by themselves to analyze the data. However, that seemed counterintuitive for the end use case of analytics and the data models' target audience. [aws_setup](https://github.com/ohjang121/project_capstone/blob/main/dags/aws_setup.py) sets up a new Redshift cluster, and various tasks in [immigration_dag.py](https://github.com/ohjang121/project_capstone/blob/main/dags/immigration_dag.py) create tables and load production data into them.

3. **AWS S3**: I used S3 for data storage. It works well with Spark and Redshift to extract and load datasets as desired.

4. **Apache Airflow**: I used Airflow to orchestrate all tasks needed to be done to design and create the data models. Instead of running separate scripts one by one, using a dependency management tool to orchestrate all necessary steps seemed like a sound option. [immigration_dag.py](https://github.com/ohjang121/project_capstone/blob/main/dags/immigration_dag.py) sets up a DAG for the immigration data processing.

#### Data Update Cadence

Even though the data pipeline is fairly light with minimal cluster configurations, the source datasets' update cadence should be considered heavily to make this decision. As both immigration and temperature data get new batch of data every month, the data pipeline should also run every month with the new datasets. Since the demographics data do not indicate a timestamp and have far less tendency and magnitude to be updated, monthly update for the data pipeline and data source would be optimal for this project. 

#### What-if Analysis

1. The data was increased by 100x: I would continue using Spark for the ETL process and add a task to set up an EMR cluster in the DAG so that it can use parallel computing instead of running it locally (which is possible now). If the business need is large enough with high number of users, I will keep Redshift as the main database to store the production tables and perform analysis there with larger computing power nodes in the configuration set up. If the use case is not as enticing, I would only enable `schema-on-read` and not use Redshift to analyze the data.

2. The pipelines were run on a daily basis by 7am: I would change the existing DAG default_args to run at 7am UTC.

3. The database needed to be accessed by 100+ people: As mentioned in the data increase 100x scenario, if the business use case is large, the cost is justified to enable ease of analysis. I will maintain the current ETL process using Spark and continue using Redshift as the main choice of database. With the end goal of enabling analytics, the data models will only be useful if they can be used by the general analyst's tech stack preference and familiarity. With Redshift's flexibility in node configurations and multiple user handling, I will encourage users to use Redshift to access the data instead of `schema-on-read` via S3.

### Next Steps / Future Considerations
* EMR Cluster set up in the DAG for more efficient spark job instead of local mode
* Airflow connection update handling programmatically instead of manual Airflow UI intervention as needed
* Gather other sources of data to do more interesting analysis
