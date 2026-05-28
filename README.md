# airflow-dataflow-orq
The file docker compose was edited for I can acess the data since terminal.
I created a directorie at airflow for the data for flowwork
# Proyecto Artist

 -/airflow/dags/etl_track

    ls -lh  ./airflow/dags/etl_track 
 ```bash
    ls -lh  ./airflow/dags/etl_track 
    8 -rw-rw-rw-  1 codespace root 5111 May 24 22:57 1_bronze_track.py
    4 -rw-rw-rw-  1 codespace root  915 May 24 22:57 1_main_etl_track.py
    4 drwxrwxrwx+ 2     50000 root 4096 May 24 23:09 __pycache__
    4 drwxrwxrwx+ 3 codespace root 4096 May 24 23:09 bronze
    4 drwxrwxrwx+ 3 codespace root 4096 May 24 23:09 gold
    4 drwxrwxrwx+ 3 codespace root 4096 May 24 23:09 silver
    4 drwxrwxrwx+ 2 codespace root 4096 May 24 22:57 sql
 ```
 
 ![Schema_silver.png](/airflow/dags/etl_track/docs/schema_silver.png)
### Bronze Airflow
 ![pipeline_bronze.png](/airflow/dags/etl_track/docs/1_bronze_track_1.png)
### Silver Airflow
 ![pipeline_silver_track.png](/airflow/dags/etl_track/docs/2_silver_track_2.png)
 ![pipeline_silver_track_transfor.png](/airflow/dags/etl_track/docs/2_silver_transform_track_3.png)
### Silver Airflow
 ![pipeline_gold_track.png](/airflow/dags/etl_track/docs/3_gold_track_4.png)
