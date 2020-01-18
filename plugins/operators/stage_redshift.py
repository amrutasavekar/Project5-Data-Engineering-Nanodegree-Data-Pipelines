import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'    
    template_fields = ("s3_key",)
    copy_sql_json = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            {} 'auto'
            COMPUPDATE OFF ;
        """
    copy_sql_csv = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            {} 'auto'
            DELIMETER ',' 
            IGNOREHEADER 1;
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 filetype="",
                 *args, 
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region= region
        self.aws_credentials_id = aws_credentials_id
        self.filetype = filetype
             

    def execute(self, context):
        """ Loading data from S3 to staging tables in redshift.
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        self.log.info("Getting AWS Credentials....")
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Connecting to redshift....")
        self.log.info(f"Create table {self.table} ......")
        rendered_key = self.s3_key.format(**context)
        print(self.s3_key) 
        print(rendered_key)
        s3_path = "s3://{}/{}".format(self.s3_bucket,rendered_key)
        self.log.info(f'{s3_path}')
       
        formatted_sql=""       
        if self.filetype == "CSV":
            formatted_sql = StageToRedshiftOperator.copy_sql_json.format(
                            self.table,
                            s3_path,
                            credentials.access_key,
                            credentials.secret_key,
                            self.region,
                            self.filetype)
                            
        elif self.filetype == "JSON":
            formatted_sql = StageToRedshiftOperator.copy_sql_json.format(
                            self.table,
                            s3_path,
                            credentials.access_key,
                            credentials.secret_key,
                            self.region,
                            self.filetype)
        
        self.log.info(formatted_sql)
        self.log.info(f"Getting contents from {self.filetype} files into staging table {self.table} from S3 to Redshift..........")
        redshift.run(formatted_sql)
        self.log.info(f"Successfully inserted records into {self.table}")




