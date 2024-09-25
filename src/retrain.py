from airflow.providers.google.cloud.operators.functions import CloudFunctionDeployFunctionOperator

def retrain_ML_model(dag) -> CloudFunctionDeployFunctionOperator:
    
    body = {
        "name": "retrainModelFunc", 
        "entryPoint": "retrain_model", 
        "runtime": "python311", 
        "httpsTrigger": {}
    }
    
    return CloudFunctionDeployFunctionOperator(
        task_id="retrain_ML_model",
        dag=dag,
        location="us-central1",
        gcp_conn_id="bq_conn",
        project_id="creditriskmlops",
        body = body 
    )