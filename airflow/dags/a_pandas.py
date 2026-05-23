from __future__ import annotations
import logging
import sys
import tempfile
import time
from pprint import pprint

import pendulum

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
)
#from airflow.providers.operators.python import PythonVirtualenvOperator, is_venv_installed


log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable


with DAG(
    dag_id="a_example_python_and_pandas",
    schedule=None,
    tags=["a_pandad_example"],
) as dag:

    #if not is_venv_installed():
    #    log.warning("The virtalenv_python example task requires virtualenv, please install it.")
    #else:
        # [START howto_operator_python_venv]
        @task.virtualenv(
            task_id="virtualenv_python", requirements=["pandas==2.2.3"], system_site_packages=False
        )
        def pandas_head():
            import pandas as pd
            csv_url = "https://raw.githubusercontent.com/paiml/wine-ratings/main/wine-ratings.csv"
            df = pd.read_csv(csv_url, index_col=0)
            head = df.head(10)
            head.to_csv("/opt/airflow/dags/datasets/head_dataset.csv")
            return head.to_csv()

        pandas_task = pandas_head()