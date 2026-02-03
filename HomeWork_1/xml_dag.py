import json
from datetime import datetime
import xml.etree.ElementTree as ET
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

@dag(
    dag_id='load_nutrition_to_postgres',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['xml', 'nutrition']
)
def nutrition_dag():
    @task
    def parse_nutrition_xml(file_path: str):
        tree = ET.parse(file_path)
        root = tree.getroot()

        rows = []
        for food in root.findall('food'):
            item = {
                'name': food.findtext('name'),
                'mfr': food.findtext('mfr'),
                'calories': int(float(food.find('calories').get('total'))) if food.find('calories') is not None else 0,
                'total_fat': int(float(food.findtext('total-fat', 0))),
                'saturated_fat': int(float(food.findtext('saturated-fat', 0))),
                'cholesterol': int(float(food.findtext('cholesterol', 0))),
                'sodium': int(float(food.findtext('sodium', 0))),
                'carb': int(float(food.findtext('carb', 0))),
                'fiber': int(float(food.findtext('fiber', 0))),
                'protein': int(float(food.findtext('protein', 0))),
            }

            vits_node = food.find('vitamins')
            vitamins_data = {}
            if vits_node is not None:
                for child in vits_node:
                    vitamins_data[child.tag] = int(child.text)
            item['vitamins'] = json.dumps(vitamins_data)

            mins_node = food.find('minerals')
            minerals_data = {}
            if mins_node is not None:
                for child in mins_node:
                    minerals_data[child.tag] = int(child.text)
            item['minerals'] = json.dumps(minerals_data)

            rows.append(item)

        return rows

    @task
    def load_to_postgres(data):
        if not data:
            return

        hook = PostgresHook(postgres_conn_id='postgres-airflow')
        df = pd.DataFrame(data)

        engine = hook.get_sqlalchemy_engine()
        df.to_sql('data_from_xml', con=engine, if_exists='append', index=False)

    file_path = "{{ dag_run.conf.get('file_path', '/tmp/nutrition.xml') }}"

    parsed_data = parse_nutrition_xml(file_path)
    load_to_postgres(parsed_data)


nutrition_dag()