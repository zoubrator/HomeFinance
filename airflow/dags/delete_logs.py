
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import shutil
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.hooks.base_hook import BaseHook

chat_id_glob = BaseHook.get_connection('telegram').host

def delete_logs():
    for root, dirs, files in os.walk(os.getenv('AIRFLOW_HOME')+'/logs'):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))

def failure(context):
    send_message = TelegramOperator(
        task_id='send_message_failure',
        telegram_conn_id='telegram',
        chat_id=chat_id_glob,
        text='Не получилось удалить логи '+context.get('task_instance').log_url.replace('localhost:8080','0.0.0.0:18081'),
        dag=dag)
    return send_message.execute(context=context)

def success(context):
    send_message = TelegramOperator(
        task_id='send_message_success',
        telegram_conn_id='telegram',
        chat_id=chat_id_glob,
        text='Логи успешно удалены',
        dag=dag)
    return send_message.execute(context=context)

dag = DAG(
    'delete_logs',
    start_date=datetime(2023, 11, 15),
    schedule_interval="0 0 1 * *")

delete_logs = PythonOperator(
    task_id='delete_logs',
    python_callable=delete_logs,
    on_failure_callback=failure,
    on_success_callback=success,
    dag=dag)

delete_logs