import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import logging
from load_prod_data import ProdQueries

### Logging Handling ###

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    logger.addHandler(sh)

# IMPORTANT: Need to update airflow root path depending on the user
airflow_root_path = '/Users/joh/airflow'
config_path = os.path.join(airflow_root_path, 'dl.cfg')
config = configparser.ConfigParser()
config.read(config_path)

logger.info(config_path)

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Instantiate a SparkSession object using Hadoop-AWS package
    Returns a SparkSession object
    '''
    spark = SparkSession \
        .builder \
        .appName("Immigration data exploration with Spark SQL") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
        .getOrCreate()
    return spark


def process_immigration_data(spark, input_path, output_path, immigration_data_path, port_mapping_data_path, country_mapping_data_path):
    '''
    Load the immigration, port_mapping, and country_mapping data stored in the input_path parameter into a Spark dataframe
    Transform staging data with pre-written queries in ProdQueries class
    Write following 2 table outputs in parquet in the output_path parameter:
        fact_immigration
        dim_immigrant
    '''

    ### fact_immigration table prep for prod load ###
    ### data formatting achieved in the SQL input ###

    # get filepath to immigration data files
    # 1 subdirectory with hashed file names - need 1 wildcard for comprehensive search
    immigration_data = os.path.join(input_path, immigration_data_path, '*.parquet')
    
    # read immigration data files
    # define temp view to query
    logger.info('Reading immigration data into stg_immigration...')
    df_imm = spark.read.parquet(immigration_data)
    df_imm.createOrReplaceTempView('stg_immigration')

    # get filepath to port mapping for arrival city and state
    port_mapping_data = os.path.join(input_path, port_mapping_data_path)

    # read port mapping data file
    # define temp view to query
    logger.info('Reading port_mapping data into port_mapping...')
    df_port = spark.read.option('header', 'true').csv(port_mapping_data)
    df_port.createOrReplaceTempView('port_mapping')

    # extract and transform columns to create fact_immigration table
    spark.sql(ProdQueries.fact_immigration_prod).createOrReplaceTempView('fact_immigration')

    # clean & format data further
    # filter any rows that do not have valid arrival city or arrival date
    fact_immigration_table = spark.sql('''
    SELECT *
    FROM fact_immigration
    WHERE arrival_city is not null
    AND arrival_date is not null
    ''')
    
    # date format combined year and month column
    #fact_immigration_table = fact_immigration_table.withColumn('imm_report_month', to_date(col('imm_report_month', 'yyyy-mm-dd')))
    
    # write fact_immigration table to parquet files
    # do not partition by arrival_date because Redshift does not load the partitioned column into the table
    fact_immigration_table.write.mode('overwrite') \
                                .parquet(os.path.join(output_path, 'fact_immigration/'))

    logger.info('Successfully wrote fact_immigration table output into parquet files!')
    
    # check data types due to copy error in redshift
    fact_immigration_table.printSchema()

    ### dim_immigrant table prep for prod load ###
    ### data formatting achieved in the SQL input ###

    # get filepath to country mapping for citizenship and residence country
    country_mapping_data = os.path.join(input_path, country_mapping_data_path)

    # read country mapping data file
    # define temp view to query
    logger.info('Reading country_mapping data into country_mapping...')
    df_country = spark.read.option('header', 'true').csv(country_mapping_data)
    df_country.createOrReplaceTempView('country_mapping')

    # extract and transform columns to create dim_immigrant table
    spark.sql(ProdQueries.dim_immigrant_prod).createOrReplaceTempView('dim_immigrant')

    # clean & format data further
    # filter any rows that do not have valid gender or age
    dim_immigrant_table = spark.sql('''
    SELECT * 
    FROM dim_immigrant 
    WHERE gender in ('M', 'F')
    AND age is not null
    ''')

    # write dim_immigrant table to parquet files
    dim_immigrant_table.write.mode('overwrite') \
                             .parquet(os.path.join(output_path, 'dim_immigrant/'))

    logger.info('Successfully wrote dim_immigrant table output into parquet files!')
    
    # check data types due to copy error in redshift
    dim_immigrant_table.printSchema()

def process_temperature_data(spark, input_path, output_path, temperature_data_path):
    '''
    Load the temperature data stored in the input_path parameter into a Spark dataframe
    Transform staging data with pre-written queries in ProdQueries class
    Write following 1 table output in parquet in the output_path parameter:
        dim_temperature
    '''
    
    # get filepath to temperature data file
    temperature_data = os.path.join(input_path, temperature_data_path)

    # read temperature data file
    # define temp view to query
    logger.info('Reading temperature data into stg_temperature...')
    df_temp = spark.read.option('header', 'true').csv(temperature_data)
    df_temp.createOrReplaceTempView('stg_temperature')
    
    # extract and transform columns to create dim_temperature table
    spark.sql(ProdQueries.dim_temperature_prod).createOrReplaceTempView('dim_temperature')

    # clean & format data further
    # filter by country = United States
    # filter any rows before 1980 to only keep necessary data
    # unique key by temp_report_month || city || country
    dim_temperature_table = spark.sql('''
    SELECT row_number() over (order by temp_report_month, city) as temp_id,
    temp_report_month,
    city,
    country,
    avg_temp,
    avg_temp_uncertainty
    FROM dim_temperature
    WHERE country = 'United States'
    AND temp_report_month >= date('1980-01-01')
    ''')
    
    # write dim_temperature table to parquet files
    dim_temperature_table.write.mode('overwrite') \
                               .parquet(os.path.join(output_path, 'dim_temperature/'))

    logger.info('Successfully wrote dim_temperature table output into parquet files!')
    
    # check data types due to copy error in redshift
    dim_temperature_table.printSchema()

def process_demographics_data(spark, input_path, output_path, demographics_data_path):
    '''
    Load the demographics data stored in the input_path parameter into a Spark dataframe
    Transform staging data with pre-written queries in ProdQueries class
    Write following 1 table output in parquet in the output_path parameter:
        dim_demographics
    '''
    
    # get filepath to demographics data file
    demographics_data = os.path.join(input_path, demographics_data_path)

    # read demographics data file
    logger.info('Reading demographics data into stg_demographics...')
    df_demo = spark.read.option('header', 'true').option('delimiter', ';').csv(demographics_data)

    # format columns in snakecase without whitespace
    # define temp view to query
    df_demo = df_demo.withColumnRenamed('City', 'city') \
        .withColumnRenamed('State Code', 'state_code') \
        .withColumnRenamed('Race', 'race') \
        .withColumnRenamed('Median Age', 'median_age') \
        .withColumnRenamed('Male Population', 'male_population') \
        .withColumnRenamed('Female Population', 'female_population') \
        .withColumnRenamed('Total Population', 'total_population') \
        .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
        .withColumnRenamed('Foreign-born', 'foreign_born') \
        .withColumnRenamed('Average Household Size', 'avg_household_size')
    
    df_demo.createOrReplaceTempView('stg_demographics') 

    # extract and transform columns to create dim_demographics table
    spark.sql(ProdQueries.dim_demographics_prod).createOrReplaceTempView('stg_demographics')

    # clean & format data further
    # add a surrogate key for primary key
    # unique by city || state_code || race
    dim_demographics_table = spark.sql('''
    SELECT row_number() over (order by city, state_code, race) as demo_id,
    city,
    state_code,
    race,
    median_age,
    male_population,
    female_population,
    total_population,
    number_of_veterans,
    foreign_born,
    avg_household_size
    FROM stg_demographics
    ''')

    # write dim_demographics table to a parquet file
    dim_demographics_table.write.mode('overwrite') \
                                .parquet(os.path.join(output_path, 'dim_demographics/'))

    logger.info('Successfully wrote dim_demographics table output into parquet files!')
    
    # check data types due to copy error in redshift
    dim_demographics_table.printSchema()

def main():

    # instantiate a spark session object
    spark = create_spark_session()

    # define input variables for S3 and data file path
    input_path = config['S3']['STAGING_DATA']
    output_path = config['S3']['PRODUCTION_DATA']

    immigration_data_path = config['DATA']['IMMIGRATION']
    port_mapping_data_path = config['DATA']['PORT_MAPPING']
    country_mapping_data_path = config['DATA']['COUNTRY_MAPPING']
    temperature_data_path = config['DATA']['TEMPERATURE']
    demographics_data_path = config['DATA']['DEMOGRAPHICS']
    
    process_immigration_data(spark, input_path, output_path, immigration_data_path, port_mapping_data_path, country_mapping_data_path)    
    process_temperature_data(spark, input_path, output_path, temperature_data_path) 
    process_demographics_data(spark, input_path, output_path, demographics_data_path) 
    
    spark.stop()

if __name__ == "__main__":
    main()
