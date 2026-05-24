import datetime
import pendulum
import os

import requests
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import textwrap

@dag(
    dag_id="1_1_bronze_track",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["ETL_track"],
) 

def Bronze_Track(): 
    @task
    def create_schema_track_raw():
        query = """
        DROP SCHEMA IF EXISTS bronze_track CASCADE;

        CREATE SCHEMA IF NOT EXISTS bronze_track;
        ;
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.executescript(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1 
        
    
    create_table_track_raw = SQLExecuteQueryOperator(
        task_id="create_table_track_raw",
        conn_id="tutorial_pg_conn",
        sql="""
            DROP TABLE IF EXISTS bronze_track.track CASCADE;
            CREATE TABLE bronze_track.track (
                id SERIAL,
                title TEXT, 
                artist TEXT, 
                album TEXT, 
                album_id INTEGER,
                count INTEGER, 
                rating INTEGER, 
                len INTEGER,
                PRIMARY KEY(id)
            );""",
    )
    @task
    def get_data():
        # NOTE: configure this as appropriate for your Airflow environment
        data_path = "/opt/airflow/data/track_raw.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        #url = "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/docs/tutorial/pipeline_example.csv"

        #response = requests.request("GET", url)

        #with open(data_path, "w") as file:
        #    file.write(response.text)

        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY bronze_track.track(title,artist,album,count,rating,len) FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",  
                file,
            )
        conn.commit()
    """
    @task
    def merge_data():
        query = '''
            INSERT INTO track.employees
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM employee.employees_temp
            ) t
            ON CONFLICT ("Serial Number") DO UPDATE
            SET
              "Employee Markme" = excluded."Employee Markme",
              "Description" = excluded."Description",
              "Leave" = excluded."Leave";
        '''
        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1
    """
    create_schema_track_raw()>>[create_table_track_raw,] >> get_data() 
    #create_schema_track_raw()>>create_schema_track()>>create_table_album>>[create_table_track_raw,create_employees_table, create_employees_temp_table] >> get_data() >> merge_data()


dag = Bronze_Track()