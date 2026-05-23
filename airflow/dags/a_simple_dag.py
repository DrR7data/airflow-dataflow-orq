from datetime import timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash  \
    import BashOperator
import time 
#from airflow.utils.dates import days_ago
import pendulum
# airflow.providers.standard.operators.bash


with DAG(
    dag_id="a_simple_dag",
    schedule=None,
    start_date=pendulum.datetime(2025, 8, 5, tz="UTC"),
    catchup=False,
    tags=["a_simple_dag"],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
        dag=dag,
    )

    t2 = BashOperator(
        task_id='create_carpeta',
        depends_on_past=False,
        bash_command='mkdir -p /opt/airflow/dags/prueba',
        dag=dag,
    )

    t3 = BashOperator(
        task_id='create_file_in_carpeta',
        depends_on_past=False,
        bash_command='echo \'Buenos días Alicia\' >> /opt/airflow/dags/prueba/alice.txt',
        dag=dag,
        )
    t4 = BashOperator(
        task_id='add_content_to_file_in_carpeta',
        depends_on_past=False,
        bash_command='echo \'Buenos días Alicia\' >> /opt/airflow/dags/prueba/alice.txt',
        dag=dag,
        )
    t5 = BashOperator(
        task_id='add_content_to_file_in_data',
        depends_on_past=False,
        bash_command='echo \'Buenos días Alicia\' >> /opt/airflow/data/alice.txt',
        dag=dag,
        )
    t6 = BashOperator(
        task_id='print_end',
        depends_on_past=False,
        bash_command='echo \'Buenos días Alicia\'',
        dag=dag,
        )

    t1 >> t2
    t2 >> t3 >> t4 >> t5 >> t6