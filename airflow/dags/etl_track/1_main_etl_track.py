from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="1_pipeline_capas_etl",
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
    tags=["ETL_track"],
) as dag:

    # Tareas independientes
    bronze_task = BashOperator(task_id="create_table_track_raw", bash_command="echo 'Ejecutando Bronze'")
    silver_task_1 = BashOperator(task_id="1_air_silver_track", bash_command="echo 'Ejecutando Silver'")
    silver_task_2= BashOperator(task_id="1_air_silver_transf_track", bash_command="echo 'Ejecutando Silver'")
    #gold_task = BashOperator(task_id="load_gold", bash_command="echo 'Ejecutando Gold'")

    # Se ejecutan al mismo tiempo (todas dependen del inicio)
    start = BashOperator(task_id="inicio", bash_command="echo 'Iniciando proceso'")
    
    start >> [bronze_task] >> silver_task_1 >> silver_task_2