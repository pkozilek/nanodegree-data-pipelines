from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, redshift_conn_id, output_table, input_query, truncate=False, *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.output_table = output_table
        self.input_query = input_query

    def execute(self, context):
        self.log.info('Setting redshift connection')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info('Transforming and loading data on Redshift')
        redshift_hook.run(
            f"""
                INSERT INTO {self.output_table}
                {self.input_query}
            """
        )
