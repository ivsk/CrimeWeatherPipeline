from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from sodapy import Socrata
import pandas as pd
from io import StringIO
import boto3
import configparser


class SocrataToS3Operator(BaseOperator):
    """
    Implements the process of pulling the data from the Chicago Data Portal using Socrata API and loading it into S3.

    Attributes
    ----------
    aws_credentials_id : str
        AWS credentials
    folder : str
        target S3 bucket folder

    Methods
    -------
    execute():
        Executes the data pulling from Chicago Data portal and loading it into S3.
    """

    ui_color = '#358140'


    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 folder="",
                 provide_context=True,
                 *args, **kwargs):
        super(SocrataToS3Operator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.folder = folder
        self.provide_context = provide_context


    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type="s3")
        self.log.info("Getting AWS credentials")
        credentials = aws_hook.get_credentials()


        year = context["data_interval_start"].date().year
        month = context["data_interval_start"].date().strftime('%m')

        self.log.info("Pulling crime data via Socrata API")

        config = configparser.ConfigParser()
        config.read_file(open('dags/support/dwh.cfg'))
        socrata_key = config.get('SOCRATA', 'key')

        client = Socrata('data.cityofchicago.org', socrata_key)
        query = f"""
            select
               date, block, primary_type, description,
               arrest, domestic, district, ward, community_area
            where
               date_extract_y(date) = '{year}'
               and date_extract_m(date) = '{month}'
            limit 30000
            """

        results = client.get("crimes", query=query)

        results_df = pd.DataFrame.from_records(results)
        self.log.info("{}".format(results_df.head()))
        s3 = boto3.client('s3', aws_access_key_id=credentials.access_key, aws_secret_access_key=credentials.secret_key)
        csv_buffer = StringIO()
        results_df.to_csv(csv_buffer)
        rendered_key = "{}/{}-{}.csv".format(self.folder, year, month)
        s3.put_object(Bucket="udacitycapstoneprojectbucket", Key=rendered_key, Body=csv_buffer.getvalue())

        self.log.info("Successfully loaded crime data to S3".format(self.folder))

