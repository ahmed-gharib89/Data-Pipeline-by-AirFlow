from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.checks:
            query = check.get('query')
            expected = check.get('expected')
            result = redshift.get_records(query)
            self.log.info(result)

            if expected == '+':
                if int(result[0][0]) > 0:
                    self.log.info(f"Cheack passed")
                else:
                    raise ValueError(f"Expected positive value for row counts but got {result[0][0]} instead")
                
            elif expected == '0':
                if int(result[0][0]) == 0:
                    self.log.info(f"Cheack passed")
                else:
                    raise ValueError(f"Expected 0 for row counts but got {result[0][0]} instead")
                


        
