import os
import requests
from airflow.sdk import task, DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id="a_load_data_postgres",
    schedule=None,
    tags=["a_postgres_example"],
) as dag:
    @task
    def get_data():
        # NOTE: configure this as appropriate for your Airflow environment
        data_path = "/opt/airflow/dags/files/employees.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/docs/tutorial/pipeline_example.csv"

        response = requests.request("GET", url)

        with open(data_path, "w") as file:
            file.write(response.text)

        postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()