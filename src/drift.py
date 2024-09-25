from datetime import (
    datetime,
    timedelta,
    date
)

from airflow.models import Variable
from google.oauth2 import service_account
from google.cloud.bigquery import Client
from pandas import DataFrame
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from airflow.operators.python import BranchPythonOperator


def detect_data_drift() -> str:
    last_week_interval = [
        date.today(), 
        (datetime.now() - timedelta(weeks=1)).date()  # last day from last week
    ]
    last_month_interval = [
        last_week_interval[1] - timedelta(days=1),
        (last_week_interval[1] - timedelta(days=1)) - timedelta(weeks=4)
    ]
    
    base_sql_query = """
        SELECT
            *
        FROM
            `creditriskmlops.credit_risk_landing.revised-credit-risk`
        WHERE
            date BETWEEN "{}" AND "{}" 
    """  # date BETWEEN "{oldest_in_interval}" AND "{newest_in_interval}" 
    
    bq_conn_json: dict = Variable.get("bq_conn_var", deserialize_json=True)
    
    bq_credentials = service_account.Credentials.from_service_account_info(bq_conn_json)
    
    bq_client = Client(
        credentials= bq_credentials,
        project=bq_conn_json["project_id"]
    )
    
    last_week_data: DataFrame = (
        bq_client.query(base_sql_query.format(last_week_interval[1], last_week_interval[0]))
        .to_dataframe()
    )
    last_month_data: DataFrame = (
        bq_client.query(base_sql_query.format(last_month_interval[1], last_month_interval[0]))
        .to_dataframe()
    )
    
    if len(last_week_data) == 0 or len(last_month_data) == 0:
        return "retrain_ML_model_end" # can't detect drift if there's no data to be compared with
    
    data_drift_report = Report(metrics=[DataDriftPreset()])
    data_drift_report.run(
        reference_data=last_month_data, current_data=last_week_data
    )
    drift_detected: bool = data_drift_report.as_dict()["metrics"][0]["result"]["dataset_drift"]
    next_step = "retrain_ML_model_end" if not drift_detected else "retraining_ML_model"
    return next_step


def detect_drift_step(dag):
    detect_drift = BranchPythonOperator(
        task_id = "detect_drift",
        python_callable=detect_data_drift,
        dag=dag
    )
    return detect_drift