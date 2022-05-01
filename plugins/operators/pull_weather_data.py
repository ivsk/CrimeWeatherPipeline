from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import pandas as pd
from google.cloud import bigquery
import os
from io import StringIO
import boto3
class BigQueryToS3Operator(BaseOperator):
    """
    Implements the process of pulling and cleaning the weather data from the BigQuery and loading it into S3.

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
        super(BigQueryToS3Operator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.folder = folder
        self.provide_context = provide_context

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type="s3")
        self.log.info("Getting AWS credentials")
        credentials = aws_hook.get_credentials()

        SERVICE_ACCOUNT_JSON = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

        self.log.info("Pulling data from BigQuery")
        year = context["data_interval_start"].date().year
        month = context["data_interval_start"].date().strftime('%m')
        QUERY = f"""
            SELECT year, mo, da, temp, wdsp, fog, rain_drizzle, snow_ice_pellets, thunder FROM bigquery-public-data.noaa_gsod.gsod{year} 
                    WHERE stn like '725340' AND mo like '{month}'
                    """
        query_job = client.query(QUERY)
        query_result = query_job.result()
        results_df = query_result.to_dataframe()
        results_df["thunder"].replace(["1000","10"],"1", inplace =True)
        results_df["snow_ice_pellets"].replace("10", "1", inplace=True)
        self.log.info("{}".format(results_df.head()))

        s3 = boto3.client('s3', aws_access_key_id=credentials.access_key, aws_secret_access_key=credentials.secret_key)
        csv_buffer = StringIO()
        results_df.to_csv(csv_buffer)
        rendered_key = "{}/{}-{}.csv".format(self.folder, year, month)
        s3.put_object(Bucket="udacitycapstoneprojectbucket", Key=rendered_key, Body=csv_buffer.getvalue())