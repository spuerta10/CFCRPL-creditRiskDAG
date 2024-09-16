from datetime import (
    datetime,
    timedelta,
    date
)

from airflow.models.taskinstance import TaskInstance 
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryGetDataOperator,
    BigQueryDeleteTableOperator
)
from airflow.operators.python import BranchPythonOperator


def check_if_retraining_data_is_sufficient(
    **kwargs
) -> str:
    print(kwargs)
    task_instance: TaskInstance = kwargs["ti"]
    last_week_revised_credit_petitions: list[list[str]] = task_instance.xcom_pull(task_ids="check_retraining_data_threshold.pull_last_week_revised_credit_petitions_number")
    last_week_revised_credit_petitions_number = int(last_week_revised_credit_petitions[0][0])
    next_step: str = "ingest_last_week_revised_credit_petitions.query_last_week_revised_credit_petitions" if last_week_revised_credit_petitions_number > 12 \
        else "retrain_ML_model_end"
    return next_step 


def create_retraining_data_threshold(dag) -> TaskGroup:
    with TaskGroup(group_id="check_retraining_data_threshold", dag=dag) as check_retraining_data_threshold:
        last_execution = str(
            (datetime.now() - timedelta(weeks=1)).date()
        )  # assuming the DAG will be executed weekly
        
        find_last_week_petitions = BigQueryExecuteQueryOperator(
            task_id = "count_last_week_revised_credit_petitions",
            sql= f"""
                SELECT 
                    COUNT(date) AS petitions
                FROM 
                    `creditriskmlops.credit_risk_landing.revised-credit-risk` 
                WHERE 
                    date BETWEEN "{last_execution}" AND "{str(date.today())}"
            """,  # better to store this in a config file
            destination_dataset_table="credit_risk_landing.airflow-temp",  # better to store this in a config file
            write_disposition= "WRITE_TRUNCATE",
            gcp_conn_id="bq_conn",  # better to store this in a config file
            use_legacy_sql=False
        )
        
        pull_last_week_petitions_number = BigQueryGetDataOperator(
            task_id = "pull_last_week_revised_credit_petitions_number",
            dataset_id="credit_risk_landing",
            table_id="airflow-temp",
            max_results=1,
            selected_fields="petitions",
            gcp_conn_id="bq_conn"  # better to store this in a config file
        )
        
        delete_temp_table = BigQueryDeleteTableOperator(
            task_id = "delete_bq_temp_table",
            deletion_dataset_table= "creditriskmlops.credit_risk_landing.airflow-temp",
            gcp_conn_id= "bq_conn"
        ) 
        
        should_retrain_model = BranchPythonOperator(
            task_id="check_if_retraining_data_is_enough",
            python_callable=check_if_retraining_data_is_sufficient
        )
        
        find_last_week_petitions >> pull_last_week_petitions_number
        pull_last_week_petitions_number >> delete_temp_table
        delete_temp_table >> should_retrain_model
    
    return check_retraining_data_threshold