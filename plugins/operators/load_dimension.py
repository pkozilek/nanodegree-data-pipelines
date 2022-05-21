from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, redshift_conn_id, output_table, input_query, truncate=False, *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.output_table = output_table
        self.input_query = input_query
        self.truncate = truncate

    def execute(self, context):
        self.log.info('Setting redshift connection')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.truncate:
            redshift_hook.run(f"TRUNCATE TABLE {self.output_table}")

        self.log.info('Transforming and loading data on Redshift')
        redshift_hook.run(
            f"""
                INSERT INTO {self.output_table}
                {self.input_query}
            """
        )
