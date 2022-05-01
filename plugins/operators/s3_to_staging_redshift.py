from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

class StageToRedshiftOperator(BaseOperator):
    """
    Implements the process of pulling the data from S3 and loading them into staging tables in Redshift.

    Attributes
    ----------
    redshift_conn_id : str
        redshift connection id
    aws_credentials_id : str
        AWS credentials
    table : str
        target staging table
    s3_bucket : str
        the source S3 bucket that contains the raw data
    s3_key : str
        path to the S3 subfolder that contains the raw data


    Methods
    -------
    execute():
        Executes the data pulling and loading from S3 to Redshift
    """

    ui_color = '#436EEE'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
        CSV
        COMPUPDATE OFF
        IGNOREHEADER 1
        timeformat 'YYYY-MM-DDTHH:MI:SS'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 source_key = "",
                 target_table="",
                 s3_bucket="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.source_key = source_key

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='redshift')
        self.log.info("Getting AWS credentials")
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        year = context["data_interval_start"].date().year
        month = context["data_interval_start"].date().strftime('%m')
        self.log.info("Copying data from S3 to Redshift table {}".format(self.target_table))
        rendered_key = "{}/{}-{}.csv".format(self.source_key, year, month)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.target_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key)

        redshift.run(formatted_sql)

        self.log.info("Successfully copied table {} to Redshift".format(self.target_table))
