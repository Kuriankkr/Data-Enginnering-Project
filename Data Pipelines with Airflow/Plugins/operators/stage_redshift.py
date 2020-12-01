from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

## Note log and song data sets would have the same column as staging_songs and staging_events. They are all the same columns. 



class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
            
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            json '{}';
            
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 json = "auto",
                 region = "",
                 ignore_headers = 1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
       
    
    def execute(self, context):
        """
            Copy Data from s3 buckets to redshift cluster into staging tables
            - redshift_conn_id: redshift cluster connection
            - aws_credentials_id: AWS connection
            - table: redshift cluster name
            - s3_bucket: S3 bucket holding name
            - s3_key: S3 key files  (This is the log file or song data)
        
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(" Clearing data from destination from Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from s3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket,rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json
        )
        
        redshift.run(formatted_sql)
        
        
        
        
        
        






