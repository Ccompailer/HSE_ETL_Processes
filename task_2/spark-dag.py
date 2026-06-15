import logging
import pendulum
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.providers.yandex.hooks.yandex import YandexCloudBaseHook
from grpc import RpcError

from yandexcloud.operations import OperationError


YANDEX_CONN_ID = 'yandexcloud_default'

# Данные инфраструктуры
FOLDER_ID = 'b1g7dgb8jfm5d87lsdea'
SERVICE_ACCOUNT_ID = 'aje7iv3b4bo0nn9gapol'
SUBNET_IDS = ['enpi214vpt9jfsojljts']
SECURITY_GROUP_IDS = ['enpi6ldmjlc06jo0489e']
METASTORE_CLUSTER_ID = 'c9q846hdgv80gnk7icr9'

JOB_NAME = 'script'
JOB_SCRIPT = 's3a://etl-baket/scripts/script.py'
JOB_ARGS = []
JOB_PROPERTIES = {
    'spark.executor.instances': '1',
    'spark.sql.warehouse.dir': 's3a://etl-baket/warehouse',
}


@task
# 1 этап: создание кластера Apache Spark™
def create_cluster(yc_hook, cluster_spec):
    # Полезно залогировать конфиг, чтобы проверить, что туда прилетает
    logging.info(f"Отправка спецификации кластера: {cluster_spec}")

    spark_client = yc_hook.sdk.wrappers.Spark()

    try:
        # Запускаем создание
        operation = spark_client.create_cluster(cluster_spec)

        # Если вы используете wrappers.Spark(), метод обычно возвращает объект операции.
        # Чтобы дождаться завершения, врапперы YC часто требуют вызова ожидания,
        # но если вы хотите просто забрать ID создающегося кластера:
        logging.info("Запрос на создание успешно отправлен в Yandex Cloud.")

    except RpcError as grpc_err:
        # Перехватываем gRPC ошибку и выводим ВСЕ детали
        logging.error(f"gRPC ошибка при вызове create_cluster: Код={grpc_err.code()}, Детали={grpc_err.details()}")
        raise RuntimeError(f"Яндекс API вернул ошибку: {grpc_err.details()}") from grpc_err

    except OperationError as job_error:
        logging.error(f"Ошибка выполнения операции в облаке: {job_error}")
        cluster_id = getattr(job_error.operation_result.meta, 'cluster_id', None)
        if cluster_id:
            logging.info(f"Удаляем частично созданный кластер: {cluster_id}")
            spark_client.delete_cluster(cluster_id=cluster_id)
        raise

    # Внутри wrappers.Spark() id кластера обычно сохраняется в поле _cluster_id или берется из операции
    # Убедитесь, что этот атрибут существует, либо достаньте его напрямую
    cluster_id = getattr(spark_client, 'cluster_id', None)
    logging.info(f"Кластер успешно инициирован с ID: {cluster_id}")
    return cluster_id


@task
# 2 этап: запуск задания PySpark
def run_spark_job(yc_hook, cluster_id, job_spec):
    spark_client = yc_hook.sdk.wrappers.Spark()
    try:
        job_operation = spark_client.create_pyspark_job(cluster_id=cluster_id, spec=job_spec)
        job_id = job_operation.response.id
        job_info = job_operation.response
    except OperationError as job_error:
        job_id = job_error.operation_result.meta.job_id
        job_info, _ = spark_client.get_job(cluster_id=cluster_id, job_id=job_id)
        raise
    finally:
        job_log = spark_client.get_job_log(cluster_id=cluster_id, job_id=job_id)
        for line in job_log:
            logging.info(line)
        logging.info("Job info: %s", job_info)


@task(trigger_rule="all_done")
# 3 этап: удаление кластера Apache Spark™
def delete_cluster(yc_hook, cluster_id):
    if cluster_id:
        spark_client = yc_hook.sdk.wrappers.Spark()
        spark_client.delete_cluster(cluster_id=cluster_id)


# Настройки DAG
with DAG(
    dag_id="example_spark",
    start_date=pendulum.datetime(2026, 1, 1),
    schedule=None,
):
    yc_hook = YandexCloudBaseHook(yandex_conn_id=YANDEX_CONN_ID)

    cluster_spec = yc_hook.sdk.wrappers.SparkClusterParameters(
        folder_id=FOLDER_ID,
        service_account_id=SERVICE_ACCOUNT_ID,
        subnet_ids=SUBNET_IDS,
        security_group_ids=SECURITY_GROUP_IDS,
        driver_pool_resource_preset="c2-m8",
        driver_pool_size=1,
        executor_pool_resource_preset="c4-m16",
        executor_pool_min_size=1,
        executor_pool_max_size=2,
        metastore_cluster_id=METASTORE_CLUSTER_ID,
    )
    cluster_id = create_cluster(yc_hook, cluster_spec)

    job_spec = yc_hook.sdk.wrappers.PysparkJobParameters(
        name=JOB_NAME,
        main_python_file_uri=JOB_SCRIPT,
        args=JOB_ARGS,
        properties=JOB_PROPERTIES,
    )
    task_job = run_spark_job(yc_hook, cluster_id, job_spec)
    task_delete = delete_cluster(yc_hook, cluster_id)

    task_job >> task_delete