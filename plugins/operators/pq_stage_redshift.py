from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class PqStageToRedshiftOperator(BaseOperator):
    '''
    Operator to load data from S3 to Amazon Redshift
    Params:
        table: Destination table to stage data into
        s3_bucket: S3 bucket name to get data from
        s3_prefix: S3 prefix name to get data from
        copy_options: Special copy option for the data
        redshift_conn_id (default = 'redshift'): Redshift connection id based on Airflow connections set up
        aws_credentials_id (default = 'aws_credentials'): AWS connection id based on Airflow connections set up
    '''
    
    ui_color = '#358140'
    #template_fields = ('s3_key',) # use if s3_key is used and needs to be rendered

    @apply_defaults
    def __init__(self,
                 table,
                 s3_bucket,
                 s3_prefix,
                 copy_options,
                 redshift_conn_id='redshift_capstone',
                 aws_credentials_id='aws_credentials',
                 *args,
                 **kwargs):                 

        super(PqStageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.copy_options = copy_options
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id, client_type='redshift') 
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id) # may need to replace to (postgres_conn_id=self.redshift_conn_id)

        # truncate table to clear before copy
        self.log.info(f'Clearing data from destination Redshift table {self.table}...')
        try:
            redshift_hook.run(f'TRUNCATE TABLE {self.table}')
            self.log.info(f'Successfully truncated table {self.table}')
        except Exception as e:
            self.log.error(e)

        # stage data from S3 to Redshift
        self.log.info(f'Staging data from S3 to Redshift for table {self.table}...')
        s3_path = f's3://{self.s3_bucket}/{self.s3_prefix}'

        copy_query = f'''
                    COPY {self.table}
                    FROM '{s3_path}'
                    ACCESS_KEY_ID '{credentials.access_key}'
                    SECRET_ACCESS_KEY '{credentials.secret_key}'
                    {self.copy_options}
                    '''
            
        self.log.info(f'Query to stage data from S3 to Redshift for table {self.table}: {copy_query}')

        try:
            redshift_hook.run(copy_query)
            self.log.info(f'Successfully staged data from S3 to Redshift for table {self.table}!')
        except Exception as e:
            self.log.error(e)
        






