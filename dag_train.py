from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
#from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookHook
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator

import os

path = os.path.dirname(os.path.abspath(__file__))
path_model=os.path.join(path, 'src/train.py')


params = {
    'path_model': path_model}

with DAG(
    'soccerguru_train_model',
    description = '2 step elt: API call + Preprocess',
    #“At 13:00 on Friday.”    
    schedule_interval='0 12 * * 5',
    start_date = days_ago(1),
    tags=["football"],
) as dag:

    alert = DiscordWebhookOperator(
        task_id= "discord_alert_start",
        http_conn_id = 'discord',
        webhook_endpoint ='webhooks/1030306654106951731/5MHkAQZMKDMUn30n1HjL-BHtDSVU5QkQFK7sZQmBXVhWtK4I-SzI97E0g2u85gjVzuNS', 
        message = 'DAG for model training started',)

t1 = BashOperator(
    task_id='train_model',
    depends_on_past=False,
    params=params,
    bash_command='python3 {{params.path_model}}',
    dag=dag)


alert2 = DiscordWebhookOperator(
    task_id= "discord_alert_finish",
    http_conn_id = 'discord',
    webhook_endpoint ='webhooks/1030306654106951731/5MHkAQZMKDMUn30n1HjL-BHtDSVU5QkQFK7sZQmBXVhWtK4I-SzI97E0g2u85gjVzuNS', 
    message = 'DAG for model training finished',
    dag=dag)



t1 >> alert2