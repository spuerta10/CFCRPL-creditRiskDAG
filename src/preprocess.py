from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
from pandas import DataFrame, concat
from sklearn.preprocessing import LabelEncoder


def handle_duplicates_and_nans(**kwargs) -> DataFrame:
    task_instance: TaskInstance = kwargs["ti"]
    last_week_revised_credit_petitions: list[dict] = task_instance.xcom_pull(task_ids="ingest_last_week_revised_credit_petitions.pull_last_week_revised_credit_petitions_number")
    print(f"Gotten BQ shape {len(last_week_revised_credit_petitions)}")
    schema = {
        "person_age":"int64",
        "person_income":"int64",
        "person_home_ownership":"string",
        "person_emp_length":"int64",
        "loan_intent":"string",
        "loan_grade":"string",
        "loan_amnt":"int64",
        "loan_int_rate":"float64",
        "loan_status":"int64",
        "loan_percent_income":"float64",
        "cb_person_default_on_file":"string",
        "cb_person_cred_hist_length":"int64",
        "revised_loan_status":"int64"
    }
    df = (
        DataFrame(
            last_week_revised_credit_petitions,
            columns = list(schema.keys())
        )
        .astype(schema)
    )
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df = df.convert_dtypes()
    print(f"Returned shape {df.size}")
    return df


def handle_outliers(**kwargs) -> DataFrame:
    task_instance: TaskInstance = kwargs["ti"]
    df: DataFrame = task_instance.xcom_pull(task_ids="preprocess_last_week_revised_credit_petitions.resolve_duplicates_and_nans")
    print(f"Gotten shape {df.shape}")
    for col in df.select_dtypes(include=["Int64", "Float64"]).columns:
        quartiles = df[col].quantile([0.25, 0.75])
        iqr = quartiles[0.75] - quartiles[0.25]
        sever_lower_outliers = quartiles[0.25] -3 * iqr
        sever_upper_outliers = quartiles[0.25] +3 * iqr
        if sever_upper_outliers > 0 and sever_lower_outliers > 0:  # if there are outliers, get rid of 'em
            df = df[(df[col] > sever_lower_outliers)&(df[col] < sever_upper_outliers)]
    print(f"Returned shape {df.shape}")
    return df


def encode_data(**kwargs) -> TaskGroup:
    task_instance: TaskInstance = kwargs["ti"]
    df: DataFrame = task_instance.xcom_pull(task_ids="preprocess_last_week_revised_credit_petitions.resolve_outliers")
    encoder = LabelEncoder()
    encoded_df = df.select_dtypes(include=["string"]).apply(lambda x: encoder.fit_transform(x))
    encoded_df.columns = [f"encoded_{col}" for col in encoded_df.columns]
    data = concat([data, encoded_df], axis =1)
    print(data.sample(3))  ###### TODO: Del this
    return data.drop(data.select_dtypes(include=["string"]), axis=1)
    

def preprocess_last_week_revised_credit_petitions(dag) -> TaskGroup:
    with TaskGroup(group_id="preprocess_last_week_revised_credit_petitions", dag=dag) as preprocess_last_week_revised_credit_petitions:
        resolve_duplicates_and_nans = PythonOperator(
            task_id = "resolve_duplicates_and_nans",
            python_callable = handle_duplicates_and_nans
        )
        
        resolve_outliers = PythonOperator(
            task_id = "resolve_outliers",
            python_callable= handle_outliers
        )
        
        data_encoding = PythonOperator(
            task_id = "data_encoding",
            python_callable= encode_data
        )
        
        resolve_duplicates_and_nans >> resolve_outliers
        resolve_outliers >> data_encoding
    
    return preprocess_last_week_revised_credit_petitions