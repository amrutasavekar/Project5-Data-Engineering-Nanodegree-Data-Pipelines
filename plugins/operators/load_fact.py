from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#80BD9E'
   
    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                query="",
                mode="",
                *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.mode = mode
        self.query = query

    def execute(self, context):
    
        redshift =PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        self.log.info(f"Loading table  {self.table} ...")
        self.log.info('Insert fact table')
        formatted_sql=""
        if self.mode == "append":
            formatted_sql = self.query
        else:
            formatted_sql = f"DELETE FROM {self.table};{self.query}"
        self.log.info(f'{self.query}')
        redshift.run(formatted_sql)
        