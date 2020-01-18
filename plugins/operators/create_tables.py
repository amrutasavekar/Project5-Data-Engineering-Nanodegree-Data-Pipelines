import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):
    ui_color = '#758140'    
    create_tables='/home/workspace/airflow/plugins/operators/create_queries.sql'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, 
                 **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
       
        """
           Create all the tables required for data pipelines on redshift cluster DB
           AWS Credentials: aws_credentials
           redshift cluster details: redshift_conn_id
           
        """
   
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Connecting to redshift....")
        sql_statements=""
        with open(CreateTableOperator.create_tables, 'r') as sql:
            for statement in sql.read().split(";"):
                if(statement):
                    self.log.info(statement)
                    redshift.run(statement)
        
        self.log.info(f"Successfully created table: ")
        