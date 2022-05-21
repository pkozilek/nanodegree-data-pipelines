from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        aws_conn_id,
        redshift_conn_id,
        output_table,
        bucket_name,
        s3_key,
        region,
        truncate=False,
        *args,
        **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.output_table = output_table
        self.bucket_name = bucket_name
        self.s3_key = s3_key
        self.region = region
        self.truncate = truncate

    def execute(self, context):
        self.log.info('Setting aws connection')
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()

        self.log.info('Setting redshift connection')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.truncate:
            self.log.info("Clearing data from output_table on Redshift")
            redshift_hook.run(f"TRUNCATE TABLE {self.output_table}")

        self.log.info('Copying data from S3 to redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.bucket_name}/{rendered_key}"
        redshift_hook.run(
            f"""
                COPY {self.output_table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                REGION '{self.region}'
                FORMAT AS JSON 'auto'
            """
        )
        self.log.info('Task using StageToRedshiftOperator was finished!')
