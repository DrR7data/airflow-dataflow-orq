import datetime
import pendulum
import os

import requests
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import textwrap

SILVER = "silver_track"
BRONZE = "bronze_track"
GOLD = "gold_track"

@dag(
    dag_id="1_3_gold_track",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["ETL_track"],
) 

def Gold_Track(): 
    
    """
    @task
    def create_schema_gold_track():
        query = '''
        DROP SCHEMA gold_track CASCADE;

        CREATE SCHEMA IF NOT EXISTS gold_track;
        '''
        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.executescript(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1 
    """    
    
    create_schema_gold_track = SQLExecuteQueryOperator(
        task_id='create_schema_gold_track',
        conn_id='tutorial_pg_conn',
        sql="""
        DROP SCHEMA gold_track CASCADE;

        CREATE SCHEMA IF NOT EXISTS gold_track;
        """,
        split_statements=True, # Habilita la lectura de múltiples statements
        autocommit=True,       # Opcional: confirma cada query automáticamente
    )
        
    create_view_gold_track = SQLExecuteQueryOperator(
        task_id="create_view_gold_track",
        conn_id="tutorial_pg_conn",
        sql=f"""
            CREATE VIEW {GOLD}.track_view AS
            SELECT st.title as title, sa.title as album, sar.name as artist
            FROM {SILVER}.track st
            JOIN {SILVER}.album AS sa ON st.album_id = sa.id
            JOIN {SILVER}.tracktoartist as sta ON st.id = sta.track_id
            JOIN {SILVER}.artist as sar ON sta.artist_id = sar.id
            --LIMIT 3
            ;""",
    )
   

    create_schema_gold_track >> [create_view_gold_track]


dag = Gold_Track()