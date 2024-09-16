from datetime import datetime

from threshold import create_retraining_data_threshold
from ingest import ingest_last_week_revised_credit_petitions
from preprocess import preprocess_last_week_revised_credit_petitions

from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id = "retrain_ML_model_pipeline",
    start_date= datetime.now(),
    schedule= None
) as dag:
    start = EmptyOperator(task_id="retrain_ML_model_start")
    end = EmptyOperator(task_id="retrain_ML_model_end")
    
    check_retraining_data_threshold = create_retraining_data_threshold(dag)
    ingest_petitions = ingest_last_week_revised_credit_petitions(dag)
    preprocess_petitions = preprocess_last_week_revised_credit_petitions(dag)
    
    start >> check_retraining_data_threshold
    check_retraining_data_threshold >> [ingest_petitions, end]
    ingest_petitions >> preprocess_petitions