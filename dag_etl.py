from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
#from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookHook
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator

import os

path = os.path.dirname(os.path.abspath(__file__))
path_etl=os.path.join(path, 'src/etl.py')
path_preprocess=os.path.join(path, 'src/preprocess.py')


params = {
    'path_etl': path_etl,
    'path_preprocess': path_preprocess}

with DAG(
    'soccerguru_etl',
    description = '2 step elt: API call + Preprocess',
    #“At 13:00 on Friday.”    
    schedule_interval='0 13 * * 5',
    start_date = days_ago(1),
    tags=["football"],
) as dag:

    alert = DiscordWebhookOperator(
        task_id= "discord_alert_start",
        http_conn_id = 'discord',
        webhook_endpoint ='webhooks/1030306654106951731/5MHkAQZMKDMUn30n1HjL-BHtDSVU5QkQFK7sZQmBXVhWtK4I-SzI97E0g2u85gjVzuNS', 
        message = 'DAG for ETL started',)

t1 = BashOperator(
    task_id='etl_api_call',
    depends_on_past=False,
    params=params,
    bash_command='python3 {{params.path_etl}}',
    dag=dag)


t2 = BashOperator(
    task_id='preprocess_data',
    depends_on_past=False,
    params=params,
    bash_command='python3 {{params.path_preprocess}}',
    dag=dag)

alert2 = DiscordWebhookOperator(
    task_id= "discord_alert_finish",
    http_conn_id = 'discord',
    webhook_endpoint ='webhooks/1030306654106951731/5MHkAQZMKDMUn30n1HjL-BHtDSVU5QkQFK7sZQmBXVhWtK4I-SzI97E0g2u85gjVzuNS', 
    message = 'DAG for ETL finished',
    dag=dag)



t1 >> t2
t2 >> alert2