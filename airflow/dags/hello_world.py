from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.hooks.base_hook import BaseHook

chat_id_glob = BaseHook.get_connection('telegram').host

def success(context):
    send_message = TelegramOperator(
        task_id='success_message',
        telegram_conn_id='telegram',
        chat_id=chat_id_glob,
        text='Hello from airflow!',
        dag=dag)
    return send_message.execute(context=context)

dag = DAG('hello_world', start_date=datetime(2023, 11, 14))

hello_world = DummyOperator(
    task_id='hello_world',
    on_success_callback=success,
    dag=dag,
)