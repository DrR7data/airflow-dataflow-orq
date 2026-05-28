import datetime
import pendulum
import os

import requests
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import textwrap

@dag(
    dag_id="1_2_silver_track",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["ETL_track"],
) 

def Silver_Track(): 
    @task
    def create_schema_silver_track():
        query = """
        DROP SCHEMA IF EXISTS silver_track CASCADE;

        CREATE SCHEMA IF NOT EXISTS silver_track;
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
    track_schema_silver = SQLExecuteQueryOperator(
            task_id='track_schema_silver',
            conn_id='tutorial_pg_conn',
            sql="""
            DROP SCHEMA IF EXISTS silver_track CASCADE;
            
            CREATE SCHEMA IF NOT EXISTS silver_track;
            """,
            split_statements=True, # Habilita la lectura de múltiples statements
            autocommit=True,       # Opcional: confirma cada query automáticamente
    )
        
    
    create_table_silver_album = SQLExecuteQueryOperator(
        task_id="create_table_silver_album",
        conn_id="tutorial_pg_conn",
        sql="""
            DROP TABLE IF EXISTS silver_track.album CASCADE;
            CREATE TABLE silver_track.album (
                id SERIAL,  
                title VARCHAR(128) UNIQUE,
                PRIMARY KEY(id)
            );""",
    )
    create_table_silver_artist = SQLExecuteQueryOperator(
        task_id="create_table_silver_artist",
        conn_id="tutorial_pg_conn",
        sql="""
            DROP TABLE IF EXISTS silver_track.artist CASCADE;
            CREATE TABLE silver_track.artist (
                id SERIAL,
                name VARCHAR(128) UNIQUE,
                PRIMARY KEY(id)
            );""",
    )
    create_table_silver_track = SQLExecuteQueryOperator(
        task_id="create_table_silver_track",
        conn_id="tutorial_pg_conn",
        sql="""
            DROP TABLE IF EXISTS silver_track.track CASCADE;
            CREATE TABLE silver_track.track (
                id SERIAL,
                title TEXT, 
                --artist TEXT, 
                --album TEXT, 
                album_id INTEGER REFERENCES silver_track.album(id) ON DELETE CASCADE,
                count INTEGER, 
                rating INTEGER, 
                len INTEGER,
                PRIMARY KEY(id)
            );""",
    )
    
    create_table_silver_track_artist = SQLExecuteQueryOperator(
        task_id="create_table_silver_track_artist",
        conn_id="tutorial_pg_conn",
        sql="""
            DROP TABLE IF EXISTS silver_track.tracktoartist CASCADE;
            CREATE TABLE silver_track.tracktoartist (
                id SERIAL,
                track VARCHAR(128),
                track_id INTEGER REFERENCES silver_track.track(id) ON DELETE CASCADE,
                artist VARCHAR(128),
                artist_id INTEGER REFERENCES silver_track.artist(id) ON DELETE CASCADE,
                PRIMARY KEY(id)
            );""",
    )
    

    create_schema_silver_track() >> track_schema_silver >> [create_table_silver_album, create_table_silver_artist] >> create_table_silver_track >> create_table_silver_track_artist


dag = Silver_Track()