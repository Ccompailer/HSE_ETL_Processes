from datetime import datetime, timedelta
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, Param

@dag(
    dag_id='extract_stat_load_to_postgres',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['stat', 'temperature'],
    params={
        "load_type": Param("full", enum=["full", "incremental"], description="Выберите тип загрузки")
    }
)
def data_stat_dag():

    @task
    def extract_stat(params=None) -> str | None:
        load_type = params.get('load_type', 'full')
        df = pd.read_csv('/tmp/IOT-temp.csv')

        df = df[df['out/in'] == 'In'].copy()
        df['noted_date'] = pd.to_datetime(df['noted_date'], format='mixed').dt.date

        if load_type == 'incremental':
            max_date = df['noted_date'].max()
            start_date = max_date - timedelta(days=3)
            df = df[df['noted_date'] >= start_date].copy()

        low = df['temp'].quantile(0.05)
        high = df['temp'].quantile(0.95)
        df_cleaned = df[(df['temp'] >= low) & (df['temp'] <= high)].copy()

        daily_stats = df_cleaned.groupby('noted_date')['temp'].mean()
        top_5_hot = daily_stats.nlargest(5)
        top_5_cold = daily_stats.nsmallest(5)

        stats_df = pd.concat([top_5_hot, top_5_cold]).reset_index()
        stats_df['category'] = ['Hot'] * len(top_5_hot) + ['Cold'] * len(top_5_cold)

        return stats_df.to_json(date_format='iso', orient='split')

    @task
    def load_to_postgres(json_data, params=None):
        if not json_data:
            return

        load_type = params.get('load_type', 'full')
        df = pd.read_json(json_data, orient='split')
        hook = PostgresHook(postgres_conn_id='pg_conn')

        target_table = 'temperature_stat_data'
        temp_table = 'temp_weather_stats'

        engine = hook.get_sqlalchemy_engine()

        if load_type == 'full':
            df.to_sql(target_table, con=engine, if_exists='replace', index=False)
            with engine.connect() as conn:
                conn.execute(f"ALTER TABLE {target_table} ADD PRIMARY KEY (noted_date, category);")
        else:
            with engine.connect() as conn:
                df.to_sql(temp_table, con=engine, if_exists='replace', index=False)

                upsert_sql = f"""
                    INSERT INTO {target_table} (noted_date, temp, category)
                    SELECT noted_date, temp, category FROM {temp_table}
                    ON CONFLICT (noted_date, category) 
                    DO UPDATE SET 
                        temp = EXCLUDED.temp;
                """
                conn.execute(upsert_sql)

                conn.execute(f"DROP TABLE {temp_table};")

    stat_data = extract_stat()
    load_to_postgres(stat_data)

data_stat_dag()
