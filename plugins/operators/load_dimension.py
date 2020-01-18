from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
   
    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                query="",
                mode="",
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.mode = mode
    def execute(self, context):
        """
        Load specified dimension table from songplays fact table
        
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading table  {self.table} ...")
        if self.mode == "append":
            formatted_sql = self.query
        else:
            formatted_sql = f"DELETE FROM {self.table};{self.query}"
        self.log.info(f'{self.query}')
        redshift.run(formatted_sql)
    