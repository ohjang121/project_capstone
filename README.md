# Capstone Project

Objective: Combine all data engineering concepts from the Udacity Data Engineering Nanodegree Program to design and build data models from unstructured, raw datasets.

### Project Summary
In this project, I integrate immigration, temperature, and demographics data to design and build data models for analytics purpose. I chose this recommended project with my strong interest in immigration and how it is affected by other factors, being an immigrant myself.

Below are recommended steps to tackle the project:

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

#### Tools

1. **Spark**: I used PySpark to explore and assess the raw datasets. 2 reasons were 1) 2 of the datasets had few millions rows of data, which require decent computing power for fast querying, and 2) `schema-on-read` that enables SQL queries to transform the datasets as if they were already loaded in a database. [immigration_spark_etl.py](https://github.com/ohjang121/project_capstone/blob/main/dags/immigration_spark_etl.py) incorporates Spark in it.
2. **AWS Redshift**: I used Redshift to build the data models and load transformed data through Spark. End use case is for analytics, and creating a data warehouse to store production data in tables would allow the target audience of analysts to access the production data more easily. Redshift usage would not be necessary if I force the users to strictly rely on `schema-on-read` and load production data from S3 by themselves to analyze the data. However, that seemed counterintuitive for the end use case of analytics and the data models' target audience. [aws_setup](https://github.com/ohjang121/project_capstone/blob/main/dags/aws_setup.py) sets up a new Redshift cluster, and various tasks in [immigration_dag.py](https://github.com/ohjang121/project_capstone/blob/main/dags/immigration_dag.py) create tables and load production data into them.
3. **AWS S3**: I used S3 for data storage. It works well with Spark and Redshift to extract and load datasets as desired.
4. **Apache Airflow**: I used Airflow to orchestrate all tasks needed to be done to design and create the data models. Instead of running separate scripts one by one, using a dependency management tool to orchestrate all necessary steps seemed like a sound option. [immigration_dag.py](https://github.com/ohjang121/project_capstone/blob/main/dags/immigration_dag.py) sets up a DAG for the immigration data processing.


#### End Use Case

End use case for this project will be analytics - users will be able to use the data models to analyze how temperature and demographics for a given region may affect its immigration trend.

---

### Step 2: Explore and Assess the Data

Before exploring the raw datasets, I uploaded them to my public S3 bucket - `udacity-capstone-joh/staging`. This enables any user to explore the raw datasets easily without having to clone this repo or download the datasets locally. 

During the raw dataset uploading step, I extracted mapping of alphanumeric codes to country / port from [I94 Immigration Data Descriptions](https://github.com/ohjang121/project_capstone/blob/main/I94_SAS_Labels_Descriptions.SAS) into 2 separate csvs - `i94citres_country_mapping.csv` and `i94port_city_state_mapping.csv`. These mapping data are crucial to translate codified location columns in the immigration data.

[load_prod_data.py](https://github.com/ohjang121/project_capstone/blob/main/dags/load_prod_data.py) contains 4 spark sql queries that transform the raw datasets into more meaningful data with correct data type formatting. They also use the country / port mapping tables to get corresponding location values in the immigration data. Using those queries as inputs, [immigration_spark_etl.py](https://github.com/ohjang121/project_capstone/blob/main/dags/immigration_spark_etl.py) cleans and drops missing or wrong values with detailed documentation for each step (e.g. drop any rows that do not have gender = male or female). Finally, it adds surrogate keys using `row_number()` function for 2 dimensional tables that do not have primary keys.


#### Cleaning Steps

1. Transform arrdate, depdate from SAS time format to pandad.datetime
2. Parse description file to get auxiliary dimension table - country_code, city _code, state _code, mode, visa
3. Tranform city, state to upper case to match city _code and state _code table

Please refer to [Capstone_Project.ipynb](https://github.com/KentHsu/Udacity-DEND/blob/main/Capstone%20Project/Capstone_Project.ipynb).

(This step was completed in Udacity workspace as pre-steps for building up and testing the ETL data pipeline. File paths should be modified if notebook is run locally.)

---

### Step 3: Define the Data Model

#### Conceptual Data Model
Since the purpose of this data warehouse is for OLAP and BI app usage, we will model these data sets with star schema data modeling.

* Star Schema

	![alt text](https://github.com/KentHsu/Udacity-DEND/blob/main/Capstone%20Project/images/conceptual_data_model.png)

#### Data Pipeline Build Up Steps

1. Assume all data sets are stored in S3 buckets as below
	* `[Source_S3_Bucket]/immigration/18-83510-I94-Data-2016/*.sas7bdat`
	* `[Source_S3_Bucket]/I94_SAS_Labels_Descriptions.SAS`
	* `[Source_S3_Bucket]/temperature/GlobalLandTemperaturesByCity.csv`
	* `[Source_S3_Bucket]/demography/us-cities-demographics.csv`
2. Follow by Step 2 – Cleaning step to clean up data sets
3. Transform immigration data to 1 fact table and 2 dimension tables, fact table will be partitioned by state
4. Parsing label description file to get auxiliary tables
5. Transform temperature data to dimension table
6. Split demography data to 2 dimension tables
7. Store these tables back to target S3 bucket

---

### Step 4: Run Pipelines to Model the Data 

#### 4.1 Create the data model

Data processing and data model was created by Spark.

Please refer to [Capstone_Project.ipynb](https://github.com/KentHsu/Udacity-DEND/blob/main/Capstone%20Project/Capstone_Project.ipynb).

#### 4.2 Data Quality Checks

Data quality checks includes

1. No empty table after running ETL data pipeline
2. Data schema of every dimensional table matches data model

Please refer to [Data_Quality_Check.ipynb](https://github.com/KentHsu/Udacity-DEND/blob/main/Capstone%20Project/Data_Quality_Check.ipynb).

#### 4.3 Data dictionary 

![alt text](https://github.com/KentHsu/Udacity-DEND/blob/main/Capstone%20Project/images/data_dictionary.png)

---

### Step 5: Complete Project Write Up

#### Tools and Technologies
1. AWS S3 for data storage
2. Pandas for sample data set exploratory data analysis
3. PySpark for large data set data processing to transform staging table to dimensional table


#### Data Update Frequency
1. Tables created from immigration and temperature data set should be updated monthly since the raw data set is built up monthly.
2. Tables created from demography data set could be updated annually since demography data collection takes time and high frequent demography might take high cost but generate wrong conclusion.
3. All tables should be update in an append-only mode.


#### Future Design Considerations
1. The data was increased by 100x.
	
	If Spark with standalone server mode can not process 100x data set, we could consider to put data in [AWS EMR](https://aws.amazon.com/tw/emr/?nc2=h_ql_prod_an_emr&whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc) which is a distributed data cluster for processing large data sets on cloud

2. The data populates a dashboard that must be updated on a daily basis by 7am every day.

	[Apache Airflow](https://airflow.apache.org) could be used for building up a ETL data pipeline to regularly update the date and populate a report. Apache Airflow also integrate with Python and AWS very well. More applications can be combined together to deliever more powerful task automation.

3. The database needed to be accessed by 100+ people.

	[AWS Redshift](https://aws.amazon.com/tw/redshift/?nc2=h_ql_prod_db_rs&whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc) can handle up to 500 connections. If this SSOT database will be accessed by 100+ people, we can move this database to Redshift with confidence to handle this request. Cost/Benefit analysis will be needed if we are going be implement this cloud solution.

---

### Future Improvements
There are several incompletions within these data sets. We will need to collect more data to get a more complete SSOT database.

1. Immigration data set is based at 2016 but temperature data set only get to 2013 which is not enough for us to see the temperature change at 2016.
	
2. Missing state and city in label description file. This makes it hard to join immigration tables and demography tables.
