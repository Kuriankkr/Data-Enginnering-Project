from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id = "",
                 redshift_conn_id = "",
                 query = "",
                 table = "",
                 truncate_table = True,
                 *args, **kwargs):
        """
        Returns a DAG inserts data into a fact redshift table from staging tables.
        
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.truncate_table = truncate_table
        self.query = query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f'Truncating Table {self.table}')
            redshift.fun("DELETE FROM {}".format(self.table))
        
        self.log.info(f'Running query {self.query}')
        redshift.run(f"Insert into {self.table} {self.query}")
        
     
                                
        self.log.info('LoadFactOperator not implemented yet')
        
