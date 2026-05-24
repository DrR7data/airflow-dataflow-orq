import datetime
import pendulum
import os
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag

@dag(
    dag_id="2_1_ejecutar_todo_el_script",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

def BBronze_Track(): 
    ejecutar_script = SQLExecuteQueryOperator(
        task_id='2_1_ejecutar_todo_el_script',
        conn_id='tutorial_pg_conn',
        sql=open('/opt/airflow/dags/files/schema.sql').read(),
        split_statements=True, # Habilita la lectura de múltiples statements
        autocommit=True,       # Opcional: confirma cada query automáticamente

    )

    ejecutar_script

dag = BBronze_Track()