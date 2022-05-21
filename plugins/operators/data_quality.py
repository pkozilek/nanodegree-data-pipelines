from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Applies tests to check data qualities.

    Parameters
    ----------
    redshift_conn_id : object
        Redshift connection Id.
    test_cases : list
        List of tests. Each test is defined by a dictionary containing
        the following keys:
            - name: str
            - query: str
            - expected_result: int
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, redshift_conn_id, test_cases, *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        self.log.info('Setting redshift connection')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info('Starting data quality checks')
        for test in self.test_cases:
            self.log.info(f"Runing test {test['name']}")
            query_result = redshift_hook.get_records(test["query"])[0][0]
            if query_result != test["expected_result"]:
                self.log.info(query_result)
                raise ValueError(f"Data quality was reproved by test '{test['name']}'")
            else:
                self.log.info(f"Test {test['name']} passed")
