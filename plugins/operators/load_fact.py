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
                 append_data=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.append_data = append_data

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

