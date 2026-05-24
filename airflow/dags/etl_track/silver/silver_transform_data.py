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

@dag(
    dag_id="1_2_silver_transf_track",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["ETL_track"],
) 

def Silver_Track_Transform(): 
    @task
    def insert_into_silver_album_t():
        query = f"""
        INSERT INTO {SILVER}.album (title) SELECT DISTINCT album FROM {BRONZE}.track;
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1 
    
    insert_table_silver_id_track = SQLExecuteQueryOperator(
        task_id="insert_table_silver_id_track",
        conn_id="tutorial_pg_conn",
        sql=f"""
           INSERT INTO {SILVER}.track (title, album_id, count, rating, len) 
            SELECT tt.title, sa.id AS album_id, tt.count, tt.rating, len 
            FROM {BRONZE}.track tt
            JOIN {SILVER}.album AS sa ON tt.album = sa.title
            ;""",
    )
    
    """
    insert_table_silver_album_t = SQLExecuteQueryOperator(
        task_id="create_table_silver_album",
        conn_id="tutorial_pg_conn",
        sql=f'''
            UPDATE {SILVER}.track SET album_id = (SELECT album.id FROM {SILVER}.album WHERE {SILVER}.album.title = {SILVER}.album);
            ''',
    )
    """
    
    
    insert_table_silver_artist_t = SQLExecuteQueryOperator(
        task_id="create_table_silver_artist",
        conn_id="tutorial_pg_conn",
        sql=f"""
           INSERT INTO {SILVER}.artist (name) SELECT DISTINCT artist FROM {BRONZE}.track;
            """,
    )
    insert_table_silver_tracktoartist = SQLExecuteQueryOperator(
        task_id="create_table_silver_tracktoartist",
        conn_id="tutorial_pg_conn",
        sql=f"""
           INSERT INTO {SILVER}.tracktoartist (track, artist) SELECT DISTINCT title, artist FROM {BRONZE}.track
            ;""",
    )
    
    insert_table_silver_tracktoartist_id_track = SQLExecuteQueryOperator(
        task_id="insert_table_silver_tracktoartist_id_track",
        conn_id="tutorial_pg_conn",
        sql=f"""
           UPDATE {SILVER}.tracktoartist SET track_id = (SELECT track.id FROM {SILVER}.track WHERE {SILVER}.track.title = {SILVER}.tracktoartist.track)
            ;""",
    )
    
    insert_table_silver_tracktoartist_id_artist = SQLExecuteQueryOperator(
        task_id="insert_table_silver_tracktoartist_id_artist",
        conn_id="tutorial_pg_conn",
        sql=f"""
           UPDATE {SILVER}.tracktoartist SET artist_id = (SELECT artist.id FROM {SILVER}.artist WHERE {SILVER}.artist.name = {SILVER}.tracktoartist.artist)
            ;""",
    )
    

    insert_into_silver_album_t() >>insert_table_silver_id_track >> [insert_table_silver_artist_t] >> insert_table_silver_tracktoartist >>[insert_table_silver_tracktoartist_id_track, insert_table_silver_tracktoartist_id_artist]


dag = Silver_Track_Transform()