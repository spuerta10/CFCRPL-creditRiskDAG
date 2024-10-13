from datetime import date

from google.cloud.bigquery import Client
from pandas import DataFrame
from airflow import DAG
from airflow.operators.python import BranchPythonOperator


def _check_if_retrain_data_is_enough(
    bq_client: Client,
    last_execution_date: date,
    current_date: date,
    threshold: int
) -> str:
    base_query = f"""
        SELECT
            COUNT(date) AS requests
        FROM
            `creditriskmlops.credit_risk_landing.revised-credit-risk`
        WHERE
            date BETWEEN "{last_execution_date}" AND "{current_date}"
    """
    requests: DataFrame = bq_client.query(base_query) \
        .to_dataframe()  # last week requests
    
    return "detect_data_drift" if len(requests) > threshold \
        else "retrain_ML_model_end"

    
def check_retrain_data_step(
    dag: DAG,
    bq_client: Client,
    last_execution_date: date,
    current_date: date,
    threshold: int
) -> BranchPythonOperator:
    return BranchPythonOperator(
        task_id= "check_retrain_data_threshold",
        python_callable= _check_if_retrain_data_is_enough,
        op_args= [bq_client, last_execution_date, current_date, threshold],
        dag= dag
    )