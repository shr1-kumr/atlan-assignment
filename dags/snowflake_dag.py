from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

est_tz = pendulum.timezone("EST5EDT")


args = {
    "owner": "Fanatics",
    "depends_on_past": False,
    "start_date": datetime(2023, 2, 10, tzinfo=est_tz),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="snowflake_openlineage_example",
    default_args=args,
    schedule_interval='@daily',
    render_template_as_native_obj=True,  # This casts xcom variables to native types when pulled in jinja templates.
    max_active_runs=1,
    catchup=False,
)
def snowflake_example():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    connection = 'snowflake_conn'

    t1 = SnowflakeOperator(
        task_id='snowflake_if_not_exists',
    snowflake_conn_id=connection,
    sql = '''CREATE TABLE IF NOT EXISTS dw_stage.public.test_orders(
    ORD int,
        str STRING,
    num INT);'''
    )

    t2 = SnowflakeOperator(
        task_id='snowflake_insert',
        snowflake_conn_id=connection,
        sql='''INSERT
    INTO dw_stage.public.test_orders (ord, str, num) VALUES(1, 'b', 15), (2, 'a', 21),
    (3, 'b', 2)'''
    )

    t3 = SnowflakeOperator(
        task_id='snowflake_truncate',
        snowflake_conn_id=connection,
        sql='''TRUNCATE TABLE dw_stage.public.test_orders'''
    )

    start_task >> t1 >> t2 >> t3 >> end_task


snowflake_example()