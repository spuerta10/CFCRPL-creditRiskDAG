from datetime import date

from google.cloud.bigquery import Client
from pandas import DataFrame
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from airflow import DAG
from airflow.operators.python import BranchPythonOperator


def _get_drift_report(
    reference_data: DataFrame,
    current_data: DataFrame
) -> bool:
    drift_report = Report(metrics=[DataDriftPreset()])
    drift_report.run(
        reference_data= reference_data,
        current_data= current_data
    )
    return drift_report.as_dict()["metrics"][0]["result"]["dataset_drift"]


def _detect_data_drift(
    bq_client: Client,  # intialized BQ Client
    lower_interval: list[date, date],
    upper_interval: list[date, date]
) -> str: 
    base_query = """
        SELECT
            *
        FROM `creditriskmlops.credit_risk_landing.revised-credit-risk`
        WHERE
            date BETWEEN "{}" AND "{}"
    """  # date BETWEEN "{oldest_in_interval}" AND "{newest_in_interval}" 
    
    lower_interval_data: DataFrame = bq_client.query(base_query.format(*lower_interval)) \
        .to_dataframe()
    
    upper_interval_data: DataFrame = bq_client.query(base_query.format(*upper_interval)) \
        .to_dataframe()

    if (len(upper_interval < lower_interval)) or \
        (len(lower_interval_data) == 0 or len(upper_interval_data) == 0):
        return "retrain_ML_model_end"  # can't detect drift if there's no data or not enough to be compared with
    
    return "retraining_ML_model" if _get_drift_report(upper_interval, lower_interval) \
        else "retrain_ML_model_end"
    
    
def detect_drift_step(
    dag: DAG,
    bq_client: Client,
    lower_interval: list[date, date],
    upper_interval: list[date, date]
) -> BranchPythonOperator:
    return BranchPythonOperator(
        task_id= "detect_data_drift",
        python_callable= _detect_data_drift,
        op_args= [bq_client, lower_interval, upper_interval],
        dag= dag
    )