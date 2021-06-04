from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from {self.table} table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Inserting data into {self.table}")
        query = f"""
            INSERT INTO {self.table}
            {self.query}
        """
        redshift.run(query)
        self.log.info("Finished Inserting data {self.table}")

