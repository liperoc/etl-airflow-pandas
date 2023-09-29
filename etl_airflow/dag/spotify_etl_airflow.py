from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime

default_args = {
    'email':'youremail@mail.com',
    'email_on_failure': True
}

my_dag = DAG(
    'spotify_featured_playlists_dag',
    start_date=datetime(2023, 9, 28),
    schedule_interval='@daily',
    default_args=default_args
)

extract_spotify_json = BashOperator(
    task_id='extract_spotify_json', 
    bash_command='python3 /etl_airflow/scripts/extract_spotify_data.py', 
    dag=my_dag
)

dataframe_transformation = BashOperator(
    task_id='dataframe_transformation', 
    bash_command='python3 /etl_airflow/scripts/dataframe_transformation.py',
    dag=my_dag
)

save_dataframe_mysql = BashOperator(
    task_id='save_dataframe_mysql', 
    bash_command='python3 /etl_airflow/scripts/save_dataframe_to_mysql.py',
    dag=my_dag
)

email_notification = EmailOperator(
    task_id='email_notification',
    to='recipientemail@mail.com',
    subject='Database Updated Successfuly',
    html_content=""" <h3>Database Updated Successfuly</h3> """,
    dag=my_dag    
)



extract_spotify_json >> dataframe_transformation >> save_dataframe_mysql >> email_notification