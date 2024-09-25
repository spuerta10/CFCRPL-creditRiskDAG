from datetime import datetime

from retrain import retrain_ML_model
from threshold import create_retraining_data_threshold
from drift import detect_drift_step

from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id = "retrain_ML_model_pipeline",
    start_date= datetime.now(),
    schedule= None
) as dag:
    start = EmptyOperator(task_id="retrain_ML_model_start")
    end = EmptyOperator(task_id="retrain_ML_model_end")
    retrain_model = retrain_ML_model(dag)
    
    check_retraining_data_threshold = create_retraining_data_threshold(dag)
    detect_drift = detect_drift_step(dag)
    
    start >> check_retraining_data_threshold
    check_retraining_data_threshold >> [detect_drift, end]
    detect_drift >> [retrain_model, end]
    retrain_model >> end