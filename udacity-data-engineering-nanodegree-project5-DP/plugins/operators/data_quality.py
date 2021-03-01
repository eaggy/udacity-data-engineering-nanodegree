from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Check quality of the data by calculating number of rows in the table

    :param task_id: task ID
    :type task_id: str
    :param dag: DAG
    :type dag: str
    :param redshift_conn_id: Redshift connection ID
    :type redshift_conn_id:
    :param sql_query: SQL query to check
    :type sql_query: string
    :param expected_result: expected result of the check
    :type expected_result: integer
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_query='',
                 expected_result='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.expected_result = expected_result

    def execute(self, context):
        self.log.info('Read credentials')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        records = redshift_hook.get_records(self.sql_query)[0]
        if self.expected_result != records[0]:
            self.log.error(f'data quality validation failed: {records[0]} is not equal to {self.expected_result}')
            raise ValueError(f'data quality validation failed: {records[0]} is not equal to {self.expected_result}')
        else:
           self.log.info('data quality validation passed') 
        