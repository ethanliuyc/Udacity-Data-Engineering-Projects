from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 load_sql="",
                 append_data = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql = load_sql
        self.append_data = append_data

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info(f'Loading fact table {self.table} into redshift')
        
        if not self.append_data:
            self.log.info(f'Deleting table {self.table} in redshift')
            redshift_hook.run(f"DELETE FROM {self.table}")
        
        
        self.log.info(f"Inserting data from staging database into {self.table} table")
        formatted_sql = getattr(SqlQueries, self.load_sql).format(self.table)
        redshift_hook.run(formatted_sql)
