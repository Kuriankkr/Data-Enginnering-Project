from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator): ## CreateTablesOperator inherits form the BaseOperator. The BaseOperator is a class that has all the default parameters. 
    ui_color = '#80BD9E'
    sql_statement = 'create_tables.sql'
    
    @apply_defaults
    
    def __init__(self,
              redshift_conn_id ="",
              *args,**kwargs):
        
        super(CreateTablesOperator,self).__init__(*args,**kwargs)
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self,context):  ## https://knowledge.udacity.com/questions/255958
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info("Creating new tables")
        
        
        fd = open(CreateTablesOperator.sql_statement,'r')
        sql_f = fd.read()

        fd.close()
        
        sql_part = sql_f.split(';')
        
        for part in sql_part:
            if part.rstrip() != '':  ## Strips of the spaces between a sql query's ";" and "space" thing
                redshift.run(part)