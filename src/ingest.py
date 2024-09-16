from datetime import (
    datetime,
    timedelta,
    date
)

from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryGetDataOperator,
    BigQueryDeleteTableOperator
)

def ingest_last_week_revised_credit_petitions(dag) -> TaskGroup:
    with TaskGroup(group_id="ingest_last_week_revised_credit_petitions", dag=dag) as ingest_last_week_revised_credit_petitions:
        last_execution = str(
            (datetime.now() - timedelta(weeks=1)).date()
        )  # assuming the DAG will be executed weekly
        
        query_last_week_revised_credit_petitions = BigQueryExecuteQueryOperator(
            task_id = "query_last_week_revised_credit_petitions",
            sql = f"""
                SELECT
                    *
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
    
        pull_last_week_revised_credit_petitions = BigQueryGetDataOperator(
            task_id = "pull_last_week_revised_credit_petitions_number",
            dataset_id="credit_risk_landing",
            table_id="airflow-temp",
            gcp_conn_id="bq_conn",  # better to store this in a config file
            as_dict = True
        )
    
        delete_temp_table = BigQueryDeleteTableOperator(
            task_id = "delete_bq_temp_table",
            deletion_dataset_table= "creditriskmlops.credit_risk_landing.airflow-temp",
            gcp_conn_id= "bq_conn"
        ) 
    
        query_last_week_revised_credit_petitions >> pull_last_week_revised_credit_petitions
        pull_last_week_revised_credit_petitions >> delete_temp_table
    
    return ingest_last_week_revised_credit_petitions
    