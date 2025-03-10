from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults 
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 truncate_table = True,
                 query = "",
                 *args, **kwargs):
        
        """
        Returns a DAG inserts data into a dimensional redshift table from staging tables.
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.truncate_table = True
        self.query = query
        

    def execute(self, context):
        redshift = PostgresHook(postgress_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f'Truncating Table {self.table}')
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f'Running  query {self.query}')
        redshift.run(f"Insert into  {self.table} {self.query}")
        self.log.info(f"Sucess: {self.task}")
        
