from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {} FROM 's3://{}/{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {} '{}';
    """
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential_id = "",
                 table = "",
                 region = "",
                 file_format = "",
                 s3_bucket = "",
                 s3_key = "",
                 json_path = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format = file_format
        self.json_path = json_path

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credential_id)
        credential = aws_hook.get_credentials()
        
        self.log.info(f"Start deleting data from destination Redshift table {self.table}")
        redshift_hook.run(f"DELETE FROM {self.table}")
        
        self.log.info(f"Start staging data from S3 to Redshift table {self.table}")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_bucket,
            self.s3_key,
            credential.access_key,
            credential.secret_key,
            self.region,
            self.file_format,
            self.json_path)
        redshift_hook.run(formatted_sql)
        





