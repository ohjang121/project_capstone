import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from load_tables import ProdQueries

config = configparser.ConfigParser()
config.read('dl.cfg')

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
    Load the song data stored in the input_data parameter into a Spark dataframe
    Create songs and artists tables based on the song_data dataframe by writing them to parquet files
    '''

    ### fact_immigration table prep for prod load ###
    ### data formatting achieved in the SQL input ###

    # get filepath to immigration data files
    # 1 subdirectory with hashed file names - need 1 wildcard for comprehensive search
    immigration_data = os.path.join(input_path, immigration_data_path, '*.parquet')
    
    # read immigration data files
    # define temp view to query
    df_imm = spark.read.parquet(immigration_data)
    df_imm.createOrReplaceTempView('stg_immigration')

    # get filepath to port mapping for arrival city and state
    port_mapping_data = os.path.join(input_path, port_mapping_data_path)

    # read port mapping data file
    # define temp view to query
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


    ### dim_immigrant table prep for prod load ###
    ### data formatting achieved in the SQL input ###

    # get filepath to country mapping for citizenship and residence country
    country_mapping_data = os.path.join(input_path, country_mapping_data_path)

    # read country mapping data file
    # define temp view to query
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


def process_temperature_data(spark, input_path, output_path, temperature_data_path):
    '''
    Load the log data stored in the input_data parameter into a Spark dataframe
    Create users, time, and songplays tables based on the log_data dataframe by writing them to parquet files
    '''
    
    # get filepath to temperature data file
    temperature_data = os.path.join(input_path, temperature_data_path)

    # read temperature data file
    # define temp view to query
    df_temp = spark.read.option('header', 'true').csv(temperature_data)
    df_temp.createOrReplaceTempView('stg_temperature')
    
    # extract and transform columns to create dim_temperature table
    spark.sql(ProdQueries.dim_temperature_prod).createOrReplaceTempView('dim_temperature')

    # clean & format data further
    # filter by country = United States
    # filter any rows before 1980 to only keep necessary data
    # unique key by temp_report_month || city || country
    dim_temperature_table = spark.sql('''
    SELECT row_number() over (partition by temp_report_month, city, country order by avg_temp) as temp_id,
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


def process_demographics_data(spark, input_path, output_path, demographics_data_path):
    '''
    Load the log data stored in the input_data parameter into a Spark dataframe
    Create users, time, and songplays tables based on the log_data dataframe by writing them to parquet files
    '''
    
    # get filepath to demographics data file
    demographics_data = os.path.join(input_path, demographics_data_path)

    # read demographics data file
    df_demo = spark.read.option('header', 'true').option('delimiter', ';').csv(demographics_data)

    # format columns in snakecase without whitespace
    # define temp view to query
    df_demo = df_demo.withColumnRenamed('City', 'city') \
        .withColumnRenamed('State Code', 'state') \
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
    dim_demographics_table = spark.sql(ProdQueries.dim_demographics_prod)

    # clean & format data further
    # add a surrogate key for primary key
    # unique by city || state_code || race
    dim_demographics_table = spark.sql('''
    SELECT row_number() over (partition by city, state, race order by total_population) as demo_id,
    city,
    state,
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
