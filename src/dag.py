import os
from datetime import (
    datetime,
    timedelta,
    date
)
from threshold import check_retrain_data_step
from drift import detect_drift_step
from retrain import retrain_model_step

from airflow.models import Variable
from google.oauth2 import service_account
from google.cloud.bigquery import Client
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator


def _get_bq_client() -> Client:
    json_conn: dict = Variable.get("bq_conn", deserialize_json=True)
    credentials = service_account.Credentials.from_service_account_info(json_conn)
    return Client(credentials=credentials, project=json_conn["project_id"])


conf = {
    "bq_client": _get_bq_client(),
    "last_week_dates": [date.today(), (datetime.now() - timedelta(weeks=1)).date()],  # last day from last week
    "last_month_dates": [
        (datetime.now() - timedelta(weeks=1)).date() - timedelta(days=1),
        ((datetime.now() - timedelta(weeks=1)).date() - timedelta(days=1)) - timedelta(weeks=4)
    ],
    "required_records_for_retrain": 120,
    "retrain_lower_date": (datetime.now() - timedelta(weeks=1)).date(),
    "retrain_upper_date": ((datetime.now() - timedelta(weeks=1)).date() - timedelta(days=1)) - timedelta(weeks=4),
    "mlflow_remote_server": "http://127.0.0.1:5000"
}

with DAG(
    dag_id = "retrain_model_pipeline",
    start_date= datetime.now(),
    schedule= None
) as dag:
    start, end= EmptyOperator(task_id= "retrain_ML_model_start"), EmptyOperator(task_id= "retrain_ML_model_end")
    check_retrain_threshold: BranchPythonOperator= check_retrain_data_step(dag, conf["bq_client"], conf["last_week_dates"][1], date.today(), conf["required_records_for_retrain"])
    detect_data_drift: BranchPythonOperator= detect_drift_step(dag, conf["bq_client"], conf["last_week_dates"], conf["last_month_dates"])
    retrain_model = retrain_model_step(dag, conf["bq_client"], conf["retrain_lower_date"], conf["retrain_upper_date"], conf["mlflow_remote_server"])
    
    start >> check_retrain_threshold
    check_retrain_threshold >> [detect_data_drift, end]
    detect_data_drift >> [retrain_model, end]
    retrain_model >> end