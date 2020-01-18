from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables="",
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table=tables
        
    def execute(self, context):
        self.log.info('Running data quality check for ')        
        redshift_hook = PostgresHook("redshift")
        for t in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {t}")
            if len(records) < 1:
                raise ValueError(f"{t} table returned 0 results , data quality test1 failed!")
            else:   
                logging.info(f"Table {t} is not empty so data quality test1  passed!!")
            rows = records[0][0]
            if rows < 1:
                raise ValueError(f"{t} table returned 0 rows , data quality test2 failed!")
            else:
                logging.info(f"Table {t} is not empty so data quality test2  passed!!")
            
        