from datetime import date

import mlflow
from google.cloud.bigquery.client import Client
from pandas import DataFrame, concat
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import classification_report

from airflow import DAG
from airflow.operators.python import PythonOperator


def _ping_mlflow(mlflow_server_uri: str) -> bool:
    try:
        mlflow.set_tracking_uri(mlflow_server_uri)
    except Exception as e:
        raise ValueError("MLFlow remote server couldn't be reached.")
    

def _log_dataset_info(dataset_name: str, dataset: DataFrame) -> None:
    mlflow.log_param(f"{dataset_name}_dataset_shape", dataset.shape)
    mlflow.log_param(f"{dataset_name}_dataset_columns", dataset.columns)
    mlflow.log_param(f"{dataset_name}_dataset_sample", dataset.sample(5))


def _handle_outliers(data: DataFrame, columns: list[str]) -> DataFrame:
    if len(columns) == 0:
        return data
    quartiles = data[columns[0]].quantile([0.25, 0.75])
    iqr = quartiles[0.75] - quartiles[0.25]
    sever_lower_outliers = quartiles[0.25] -3 * iqr
    sever_upper_outliers = quartiles[0.25] +3 * iqr
    if sever_upper_outliers > 0 and sever_lower_outliers > 0:  # if there are outliers, get rid of 'em
        data = data[(data[columns[0]] > sever_lower_outliers)&(data[columns[0]] < sever_upper_outliers)]
    columns.pop(0)
    return _handle_outliers(data, columns)


def _perform_feature_eng(data: DataFrame) -> DataFrame:
    encoded_cols = data.select_dtypes(include=["string"]).apply(lambda x: LabelEncoder().fit_transform(x))
    encoded_cols.columns = [f"encoded_{col}" for col in encoded_cols.columns]
    data = concat([data, encoded_cols], axis =1)
    return data.drop(data.select_dtypes(include=["string"]), axis=1)


def _preprocess_data(retrain_data: DataFrame) -> DataFrame:
    _log_dataset_info("raw_retrain", retrain_data)
    retrain_data = retrain_data.convert_dtypes()
    retrain_data = retrain_data.dropna()
    retrain_data = retrain_data.drop_duplicates()
    retrain_data = _handle_outliers(retrain_data, columns=retrain_data.select_dtypes(include=["Int64", "Float64"]).columns.to_list())
    _log_dataset_info("preprocessed_retrain", retrain_data)
    retrain_data = _perform_feature_eng(retrain_data)
    _log_dataset_info("processed_retrain", retrain_data)
    return retrain_data
    

def _retrain_model(
    mlflow_server_uri: str,
    bq_client: Client,
    lower_date: date,
    upper_date: date,
) -> None:
    
    _ping_mlflow(mlflow_server_uri)  # see if MLFlow remote server can be reached
    
    base_query = f"""
        SELECT
            *
        FROM `creditriskmlops.credit_risk_landing.revised-credit-risk`
        WHERE
            date BETWEEN "{lower_date}" AND "{upper_date}"
    """
    retrain_data: DataFrame = bq_client.query(base_query) \
        .to_dataframe()
    proccesed_retrain_data: DataFrame = _preprocess_data(retrain_data)
    x, y = proccesed_retrain_data.drop(["loan_status"], axis=1), proccesed_retrain_data["loan_status"]
    x_train, x_test, y_train, y_test = train_test_split(x, y, train_size=0.7)
    xgb = XGBClassifier()
    xgb.fit(x_train, y_train)
    mlflow.xgboost.log_model(xgb, "xgb_model")
    accuracy = xgb.score(x_test, y_test)
    y_pred = xgb.predict(x_test)
    report = classification_report(y_test, y_pred)
    report_path = r"..\..\data\classification_report.txt"
    with open(report_path, "w") as file:
        file.write(report)
    mlflow.log_metric(f"accuracy", accuracy)
    mlflow.log_artifact(report_path)
    

def retrain_model_step(
    dag: DAG,
    bq_client: Client,
    lower_date: date,
    upper_date: date,
    mlflow_server_uri: str
) -> PythonOperator: 
    return PythonOperator(
        task_id= "retraining_ML_model",
        python_callable= _retrain_model,
        op_args= [mlflow_server_uri, bq_client, lower_date, upper_date],
        dag= dag
    )