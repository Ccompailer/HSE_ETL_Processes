import os
import json
import pandas as pd
from datetime import datetime, date
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy import Text

CONN_ID = 'postgres-airflow'
TEMP_PATH = '/tmp/'


@dag(
    dag_id='load_pets_json',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['json']
)
def json_dag():
    @task
    def parse_json(file_path: str):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        results = []
        for p in data.get('pets', []):
            results.append({
                'name': p.get('name'),
                'species': p.get('species'),
                'fav_foods': p.get('favFoods', []),
                'birth_date': date(p.get('birthYear'), 1, 1) if p.get('birthYear') else None,
                'photo': p.get('photo')
            })
        return results

    @task
    def load_to_db(records):
        if not records: return
        hook = PostgresHook(postgres_conn_id=f"{CONN_ID}")
        df = pd.DataFrame(records)
        engine = hook.get_sqlalchemy_engine()
        df.to_sql('data_from_json', con=engine, if_exists='append', index=False,
                  dtype={'fav_foods': ARRAY(Text)})

    path = "{{ dag_run.conf.get('file_path', '/tmp/pets.json') }}"
    load_to_db(parse_json(path))


json_dag_instance = json_dag()