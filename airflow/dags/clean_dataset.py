
from __future__ import annotations
import logging
import csv 
from datetime import time

import pendulum

from airflow.sdk import DAG, task, Asset
#from airflow.providers.standard.operators.python import is_venv_installed
from airflow.providers.standard.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
)

log = logging.getLogger(__name__)

RAW_WINE_DATASET = Asset["file://opt/airflow/dags/datasets/raw_wine_dataset.csv"]

with DAG(
    dag_id="a_a_wine_dataset_consumer",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 8, 6, tz="UTC"),
    tags=["a_example"],
) as dag:

    #if not is_venv_installed():
        #raise RuntimeError("virtualenv is not installed!")
    #else:
        @task.virtualenv(
            task_id="virtualenv_python", requirements=["pandas==2.2.3"],
            system_site_packages=False
        )
        def clean_dataset():
            import pandas as pd
            df = pd.read_csv("/opt/airflow/data/raw_wine.csv", index_col=0)
            df = df.replace({"\r": ""}, regex=True)
            df = df.replace({"\n": " "}, regex=True)
            df.drop(['grape'], axis=1, inplace=True)
            df.to_csv("/opt/airflow/data/cleaned_dataset.csv")

        @task.virtualenv(
            task_id="sqlite_persist_wine_data", requirements=["pandas==2.2.3", "sqlalchemy==2.0.41"],
            system_site_packages=False
        )
        def persist_dataset():
            import pandas as pd
            from sqlalchemy import create_engine
            engine = create_engine('sqlite:///opt/airflow/data/wine_dataset.db', echo=True)
            df = pd.read_csv("/opt/airflow/data/cleaned_dataset.csv", index_col=0)
            df.to_sql('wine_dataset', engine)
            df.notes.to_sql("wine_notes", engine)
        

        clean_dataset() >> persist_dataset()