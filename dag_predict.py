from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
#from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookHook
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator

import os

path = os.path.dirname(os.path.abspath(__file__))
path_predict=os.path.join(path, 'src/predict.py')
path_html_to_bucket=os.path.join(path, 'src/html_to_bucket')


params = {
    'path_predict': path_predict,
    'path_html_to_bucket': path_html_to_bucket}

with DAG(
    'soccerguru_predict',
    description = '2 step predict: API predict + create front',
    #“At 13:00 on Friday.”    
    schedule_interval='5 12 * * *',
    start_date = days_ago(1),
    tags=["football"],
) as dag:

    alert = DiscordWebhookOperator(
        task_id= "discord_alert_start",
        http_conn_id = 'discord',
        webhook_endpoint ='webhooks/1030306654106951731/5MHkAQZMKDMUn30n1HjL-BHtDSVU5QkQFK7sZQmBXVhWtK4I-SzI97E0g2u85gjVzuNS', 
        message = 'DAG for prediction started',)

t1 = BashOperator(
    task_id='predict_and_publish',
    depends_on_past=False,
    params=params,
    bash_command='python3 {{params.path_predict}}',
    dag=dag)

t2 = BashOperator(
    task_id='html_to_bucket',
    depends_on_past=False,
    params=params,
    bash_command='{{params.path_html_to_bucket}} ',
    dag=dag)

alert2 = DiscordWebhookOperator(
    task_id= "discord_alert_finish",
    http_conn_id = 'discord',
    webhook_endpoint ='webhooks/1030306654106951731/5MHkAQZMKDMUn30n1HjL-BHtDSVU5QkQFK7sZQmBXVhWtK4I-SzI97E0g2u85gjVzuNS', 
    message = 'DAG for prediction finished',
    dag=dag)



t1 >> t2
t2 >> alert2