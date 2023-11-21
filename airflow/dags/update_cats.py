import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

def update_cats():
    # connection
    host = BaseHook.get_connection('finance_postgres').host
    login = BaseHook.get_connection('finance_postgres').login
    psw = BaseHook.get_connection('finance_postgres').password
    postgres_engine = create_engine(f"postgresql+psycopg2://{login}:{psw}@{host}")

    opers = pd.read_sql('operations',postgres_engine)
    cats = pd.read_sql('categories', postgres_engine)
    cats_old_new = pd.read_sql('cats_old_new', postgres_engine)

    # update cats
    cats_compare = cats.merge(cats_old_new, left_on='category', right_on='category_old', how='left')
    cats_compare['category'] = cats_compare.apply(
        lambda a: a['category_new'] if a['category_new'] == a['category_new'] else a['category'], axis=1)
    cats_compare = cats_compare[['category', 'description']]
    cats_compare.to_sql('categories', postgres_engine, if_exists='replace', index=False)

    # update opers
    opers.rename(columns={'category_old': 'category'}, inplace=True)
    opers.drop(columns=['category_new'], inplace=True)
    opers = opers.merge(cats_compare, on='description', how='left', suffixes=['_old', '_new'])
    opers.to_sql('operations', postgres_engine, if_exists='replace', index=False)


dag = DAG(
    'update_cats',
    start_date=datetime(2023, 11, 9))

update_cats = PythonOperator(
    task_id='update_cats',
    python_callable=update_cats,
    dag=dag)

update_cats