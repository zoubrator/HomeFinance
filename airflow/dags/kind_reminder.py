from datetime import datetime
from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, text
from airflow.providers.telegram.operators.telegram import TelegramOperator

dag = DAG(
    'kind_reminder',
    start_date=datetime(2023, 11, 15),
    schedule_interval="0 9 * * 1")

def check_period():
    host = BaseHook.get_connection('finance_postgres').host
    login = BaseHook.get_connection('finance_postgres').login
    psw = BaseHook.get_connection('finance_postgres').password
    postgres_engine = create_engine(f"postgresql+psycopg2://{login}:{psw}@{host}")
    connection = postgres_engine.connect()
    results = connection.execute(text('SELECT max(oper_datetime) FROM operations')).fetchall()
    connection.close()
    return results[0][0]


kind_reminder = TelegramOperator(
        task_id='kind_reminder',
        telegram_conn_id='telegram',
        chat_id=BaseHook.get_connection('telegram').host,
        text=(
            "Пришло время посмотреть, сколько денег было потрачено." 
            f"\nДобавь файлы c тратами в формате csv с данными за период c {check_period()} по текущий момент."
            "\n\noperations_1.csv - кредитный счет"
            "\noperations_2.csv - дебетовый счет"
            "\n\nФайлы надо положить в /Downloads/HomeFinance/airflow/dags/data"
            "\nСделай это до 6 вечера по мск. Как только можно будет смотреть в дашборд, придет оповещение."
        ),
        dag=dag
)

kind_reminder