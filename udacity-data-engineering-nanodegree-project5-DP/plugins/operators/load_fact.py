from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Load data into fact table from staging tables 

    :param task_id: task ID
    :type task_id: str
    :param dag: DAG
    :type dag: str
    :param redshift_conn_id: Redshift connection ID
    :type redshift_conn_id: str
    :param table: target table for loading
    :type table: str
    :param select_sql: SQL query to get data for loading into target table
    :type select_sql: str
    """

    ui_color = '#F98866'
    
    insert_sql_template = """
                          INSERT INTO {table}
                          {select_sql};
                          """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 select_sql='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        self.log.info('Read credentials')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Load data into fact table')
        sql = LoadFactOperator.insert_sql_template.format(table=self.table, select_sql=self.select_sql)
        redshift_hook.run(sql)