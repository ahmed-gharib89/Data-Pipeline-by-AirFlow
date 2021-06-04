from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 query='',
                 append_data=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_data:
            self.log.info("Clearing data from {self.table} table")
            del_query = f"DELETE FROM {self.table}"
            redshift.run(del_query)

        self.log.info("Inserting data into {self.table}")
        query = f"""
            INSERT INTO {self.table}
            {self.query}
        """
        redshift.run(query)
        self.log.info("Finished Inserting data {self.table}")