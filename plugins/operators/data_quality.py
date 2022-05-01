from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
        Testing the quality of the final tables.

        Attributes
        ----------
        redshift_conn_id : str
            redshift connection id
        dq_check : list
            list of tables to test

        Methods
        -------
        execute():
            Executes the data quality check in Redshift
        """
    ui_color = '#F5DEB3'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):

        redshift = PostgresHook(self.redshift_conn_id)
        error_count = 0
        failing_tests = []
        boolean_tables = ["crime_arrest", "crime_domestic"]
        for checks in self.dq_checks:
            sql = checks["check_sql"]
            records_query = redshift.get_records(sql)[0][0]
            self.log.info("Number of records found:{}".format(records_query))
            if any(x in sql for x in boolean_tables):
                if records_query == 0 or records_query > checks["expected_result"]:
                    error_count += 1
                    failing_tests.append(sql)
            else:
                if records_query != checks["expected_result"]:
                    error_count += 1
                    failing_tests.append(sql)
        if error_count > 0:
            self.log.info("Error, query does not match expected results")
            self.log.info(failing_tests)
            raise ValueError("Data quality check has failed")

        else:
            self.log.info("Data quality check has passed on all tables")