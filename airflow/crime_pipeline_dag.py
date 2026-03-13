from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
'owner': 'crime_pipeline',
'start_date': datetime(2024, 1, 1),
'retries': 1
}

dag = DAG(
dag_id='crime_data_pipeline',
default_args=default_args,
schedule_interval='@daily',
catchup=False
)

process_data = BashOperator(
task_id='data_processing',
bash_command='python D:/crime_pipeline_project/scripts/data_processing.py',
dag=dag
)

hotspot_detection = BashOperator(
task_id='hotspot_model',
bash_command='python D:/crime_pipeline_project/scripts/hotspot_model.py',
dag=dag
)

trend_analysis = BashOperator(
task_id='trend_analysis',
bash_command='python D:/crime_pipeline_project/scripts/trend_analysis.py',
dag=dag
) 

risk_score = BashOperator(
task_id='risk_score',
bash_command='python D:/crime_pipeline_project/scripts/risk_score_model.py',
dag=dag
)

process_data >> hotspot_detection >> trend_analysis >> risk_score
