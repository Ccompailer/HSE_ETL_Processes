import logging
from datetime import datetime
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task


@dag(
    dag_id='extract_stat_load_to_postgres',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['stat', 'temperature']
)
def data_stat_dag():
    @task
    def extract_stat() -> str | None:
        df = pd.read_csv('/tmp/IOT-temp.csv')

        df = df[df['out/in'] == 'In'].copy()
        df['noted_date'] = pd.to_datetime(df['noted_date'], format='mixed').dt.date

        low = df['temp'].quantile(0.05)
        high = df['temp'].quantile(0.95)

        df_cleaned = df[(df['temp'] >= low) & (df['temp'] <= high)].copy()

        daily_stats = df_cleaned.groupby('noted_date')['temp'].mean()

        top_5_hot = daily_stats.nlargest(5)
        top_5_cold = daily_stats.nsmallest(5)

        stats_df = pd.concat([top_5_hot, top_5_cold]).reset_index()
        stats_df['category'] = ['Hot'] * 5 + ['Cold'] * 5

        return stats_df.to_json(date_format='iso', orient='split')

    @task
    def load_to_postgres(json: str | None):
        if not json:
            return

        df = pd.read_json(json, orient='split')
        hook = PostgresHook(postgres_conn_id='pg_conn')

        engine = hook.get_sqlalchemy_engine()

        logging.log(logging.WARN, f"{engine.url.password} -- {engine.url.username}")

        df.to_sql('temperature_stat_data', con=engine, if_exists='append', index=False)

    stat_data = extract_stat()
    load_to_postgres(stat_data)


data_stat_dag()
