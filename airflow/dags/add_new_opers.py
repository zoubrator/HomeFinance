import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from datetime import date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import os
from airflow.sensors.filesystem import FileSensor
from airflow.providers.telegram.operators.telegram import TelegramOperator

chat_id_glob = BaseHook.get_connection('telegram').host

def add_new_opers():
    path = os.getenv('AIRFLOW_HOME')

    path_1 = path + '/dags/data/operations_1.csv'
    path_2 = path + '/dags/data/operations_2.csv'

    if (os.path.exists(path_1)) & (os.path.exists(path_2)):
        # read and concats csv files with opers
        df1 = pd.read_csv(path_1, sep=';', encoding='1251')
        df2 = pd.read_csv(path_2, sep=';', encoding='1251')

        df1['Счет'] = 'Кредитный'
        df2['Счет'] = 'Дебетовый'

        df = pd.concat([df1, df2])

        # clean data
        df['Дата операции'] = pd.to_datetime(df['Дата операции'], format='%d.%m.%Y %H:%M:%S')
        df['Дата платежа'] = pd.to_datetime(df['Дата платежа'], format='%d.%m.%Y')
        df['Номер карты'].fillna('', inplace=True)
        df['Сумма операции'] = df['Сумма операции'].str.replace(',', '.').astype(float)
        df['Сумма платежа'] = df['Сумма платежа'].str.replace(',', '.').astype(float)
        df['Кэшбэк'].fillna(0, inplace=True)
        df['MCC'].fillna(0, inplace=True)
        df['MCC'] = df['MCC'].astype(int)
        df['Бонусы (включая кэшбэк)'] = df['Бонусы (включая кэшбэк)'].str.replace(',', '.').astype(float)
        df['Округление на инвесткопилку'] = df['Округление на инвесткопилку'].str.replace(',', '.').astype(float)
        df['Сумма операции с округлением'] = df['Сумма операции с округлением'].str.replace(',', '.').astype(float)

        df.rename(columns={'Дата операции': 'oper_datetime',
                           'Дата платежа': 'payment_date',
                           'Номер карты': 'card',
                           'Статус': 'status',
                           'Сумма операции': 'oper_money',
                           'Валюта операции': 'oper_currency',
                           'Сумма платежа': 'payment_money',
                           'Валюта платежа': 'payment_currency',
                           'Кэшбэк': 'cashback',
                           'Категория': 'category',
                           'MCC': 'mcc',
                           'Описание': 'description',
                           'Бонусы (включая кэшбэк)': 'bonus',
                           'Округление на инвесткопилку': 'round_invest',
                           'Сумма операции с округлением': 'round_money',
                           'Счет': 'account'},
                  inplace=True)

        # connection
        host = BaseHook.get_connection('finance_postgres').host
        login = BaseHook.get_connection('finance_postgres').login
        psw = BaseHook.get_connection('finance_postgres').password
        postgres_engine = create_engine(f"postgresql+psycopg2://{login}:{psw}@{host}")

        # check new categories
        cats_old = pd.read_sql('categories', postgres_engine)
        cats_new = df[['category', 'description']].groupby(['category', 'description']).count().reset_index()
        cats_compare = cats_new[~cats_new['description'].isin(cats_old['description'])]

        cats_old_new = pd.read_sql('cats_old_new', postgres_engine)
        cats_compare = cats_compare.merge(cats_old_new, left_on='category', right_on='category_old', how='left')
        cats_compare['category'] = cats_compare.apply(
            lambda a: a['category_new'] if a['category_new'] == a['category_new'] else a['category'], axis=1)
        cats_compare = cats_compare[['category', 'description']]

        cats_compare.to_sql('categories', postgres_engine, if_exists='append', index=False)

        # add new category in operations
        cats = pd.read_sql('categories', postgres_engine)
        df = df.merge(cats, on='description', how='left', suffixes=['_old', '_new'])

        # remove row that already exists in db
        operations = pd.read_sql('operations', postgres_engine)
        if not operations.empty:
            max_datetime = max(operations.oper_datetime)
            df = df[df['oper_datetime'] > max_datetime]

        # add into db
        df.to_sql('operations', postgres_engine, if_exists='append', index=False)

        # remove old files into archive
        path_1_new = path + '/dags/data/old/operations_1_' + date.today().strftime('%Y%m%d') + '.csv'
        path_2_new = path + '/dags/data/old/operations_2_' + date.today().strftime('%Y%m%d') + '.csv'
        os.replace(path_1, path_1_new)
        os.replace(path_2, path_2_new)



dag = DAG(
    'add_new_opers',
    start_date=datetime(2023, 11, 15),
    schedule_interval="0 9 * * 1"
)

wait_for_file1 = FileSensor(
    task_id='wait_for_file1',
    filepath=os.getenv('AIRFLOW_HOME')+'/dags/data/operations_1.csv',
    poke_interval=60,
    timeout=60*60*6,
    dag=dag
)

wait_for_file2 = FileSensor(
    task_id='wait_for_file2',
    filepath=os.getenv('AIRFLOW_HOME')+'/dags/data/operations_2.csv',
    poke_interval=60,
    timeout=60*60,
    dag=dag
)

def failure_add_opers(context):
    send_message = TelegramOperator(
        task_id='send_message_failure',
        telegram_conn_id='telegram',
        chat_id=chat_id_glob,
        text='Не получилось загрузить данные. Проверь логи '+context.get('task_instance').log_url.replace('localhost:8080','0.0.0.0:18081'),
        dag=dag)
    return send_message.execute(context=context)

def success_add_opers(context):
    send_message = TelegramOperator(
        task_id='send_message_success',
        telegram_conn_id='telegram',
        chat_id=chat_id_glob,
        text='Данные обновлены. Посмотри дашборд http://0.0.0.0:13000/d/eb63dc6a-d99a-428d-867f-93cef1dd8a91/rashodiki?orgId=1',
        dag=dag)
    return send_message.execute(context=context)

add_new_opers = PythonOperator(
    task_id='add_new_opers',
    python_callable=add_new_opers,
    trigger_rule='all_success',
    on_failure_callback=failure_add_opers,
    on_success_callback=success_add_opers,
    dag=dag
)

wait_for_file1 >> wait_for_file2 >> add_new_opers