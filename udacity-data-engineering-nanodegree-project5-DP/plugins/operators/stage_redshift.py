from airflow.contrib.hooks.aws_hook import AwsHook 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Load data into stage tables from JSON files located at Amazon S3 

    :param task_id: task ID
    :type task_id: str
    :param dag: DAG
    :type dag: str
    :param redshift_conn_id: Redshift connection ID
    :type redshift_conn_id: str
    :param table: target table for loading
    :type table: str
    :param s3_bucket: S3 bucket name
    :type s3_bucket: str
    :param s3_key: path in S3 bucket to JSON files
    :type s3_key: str
    :param copy_json_option: path to JSON file or 'auto'
    :type copy_json_option: str
    """
    
    ui_color = '#358140'
    
    copy_sql_template = """
                        DELETE FROM {table};
                        COPY {table}
                        FROM '{s3}'
                        ACCESS_KEY_ID '{access_key}'
                        SECRET_ACCESS_KEY '{secret_key}'
                        FORMAT AS JSON '{copy_json_option}';
                        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 copy_json_option='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option

    def execute(self, context):
        self.log.info('Read credentials')
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Copy data from S3 to Redshift')
        s3_path = "s3://{}/{}/".format(self.s3_bucket, self.s3_key)
        
        sql = StageToRedshiftOperator.copy_sql_template.format(
            table = self.table,
            s3 = s3_path,
            access_key = aws_credentials.access_key,
            secret_key = aws_credentials.secret_key,
            copy_json_option = self.copy_json_option
        )
        redshift_hook.run(sql)
        
        self.log.info(f'The table {self.table} was successfully copied from S3 to Redshift')