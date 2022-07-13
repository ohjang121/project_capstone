from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    '''
    Operator to QA output data from the data pipeline
    Params:
        table_dict: table and not null key dictionary to run check queries
        redshift_conn_id (default = 'redshift'): Redshift connection id based on Airflow connections set up
    '''

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table_dict,
                 redshift_conn_id='redshift_capstone',
                 *args, 
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_dict = table_dict
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id) # may need to replace to (postgres_conn_id=self.redshift_conn_id)
         
        # Check 1: Row count > 0
        for table, pkey in self.table_dict.items():
            records = redshift_hook.get_records(f'SELECT count(*) FROM public.{table}')
            
            # any results
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data quality check failed. public.{table} returned no results')
            
            # row check
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f'Data quality check failed. public.{table} contained 0 rows')
        
            self.log.info(f'Row count data quality check on table public.{table} passed with {num_records} records')

        # Check 2: Not null
        for table, pkey in self.table_dict.items():
            records = redshift_hook.get_records(f'SELECT count(*) FROM public.{table} where {pkey} is null')

            # row check
            num_records = records[0][0]
            if num_records > 0:
                raise ValueError(f'Data quality check failed. public.{table}.{pkey} is not null in {num_records} records')

            self.log.info(f'Not null data quality check on table public.{table} passed')

