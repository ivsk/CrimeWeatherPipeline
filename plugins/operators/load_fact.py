from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
        Loads the fact table in Redshift from the staging tables.

        Attributes
        ----------
        redshift_conn_id : str
            redshift connection id
        target_table : str
            the target dimension table
        sql : str
            SQL statement that inserts the data to the dimension tables.

        Methods
        -------
        execute():
            Executes the data transfer from staging tables to the dimension tables in Redshift
        """
    ui_color = '#388E8E'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 sql="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql_statement = 'INSERT INTO %s %s' % (self.target_table, self.sql)
        redshift.run(sql_statement)
        self.log.info('Loading data into fact table {}'.format(self.target_table))
