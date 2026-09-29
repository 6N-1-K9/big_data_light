"""
Унифицированное создание Spark-сессии.

Автоматически определяет окружение:
- если в PATH есть `yarn` (кластер Hadoop) — запуск в режиме YARN,
  каталог spark_catalog с warehouse в HDFS (/user/<user>/warehouse);
- иначе — локальный режим для разработки в docker
  (каталог `local` с warehouse в /app/warehouse).

Ресурсы можно переопределить через переменные окружения:
  SPARK_EXECUTOR_MEMORY (default на кластере 4g)
  SPARK_EXECUTOR_CORES  (default 2)
  SPARK_EXECUTOR_INSTANCES (default 2)
  SPARK_DRIVER_MEMORY   (default 2g)
"""

import os
import shutil
from pyspark.sql import SparkSession
from pyspark import SparkConf

ICEBERG_VERSION = "1.6.0"
ICEBERG_PACKAGE = (
    "org.apache.iceberg:"
    f"iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}"
)
ICEBERG_JAR_NAME = (
    "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12"
    f"-{ICEBERG_VERSION}.jar"
)


def _is_cluster() -> bool:
    """Проверяем, есть ли в системе YARN (т.е. мы на кластере Hadoop)."""
    return shutil.which("yarn") is not None


def _find_local_iceberg_jar() -> str | None:
    """Ищем заранее скачанный jar Iceberg в локальном ivy-кеше."""
    candidates = [
        os.path.expanduser(f"~/.ivy2/jars/{ICEBERG_JAR_NAME}"),
        f"/opt/spark/jars/{ICEBERG_JAR_NAME}",
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


def create_spark_configuration(app_name: str = "BigDataLab") -> SparkConf:
    conf = SparkConf()
    conf.setAppName(app_name)
    conf.set("spark.sql.extensions",
             "org.apache.iceberg.spark.extensions."
             "IcebergSparkSessionExtensions")

    if _is_cluster():
        user_name = os.getenv("USER", "user")
        conf.setMaster("yarn")
        conf.set("spark.submit.deployMode", "client")

        # Iceberg: предпочитаем готовый jar из кеша, чтобы не ходить
        # в Maven за ним; если не найден — отдаёмся на волю ivy.
        jar = _find_local_iceberg_jar()
        if jar:
            conf.set("spark.jars", jar)
        else:
            conf.set("spark.jars.packages", ICEBERG_PACKAGE)

        conf.set("spark.sql.catalog.spark_catalog",
                 "org.apache.iceberg.spark.SparkCatalog")
        conf.set("spark.sql.catalog.spark_catalog.type", "hadoop")
        conf.set("spark.sql.catalog.spark_catalog.warehouse",
                 f"hdfs:///user/{user_name}/warehouse")
        conf.set("spark.sql.catalog.spark_catalog.io-impl",
                 "org.apache.iceberg.hadoop.HadoopFileIO")

        conf.set("spark.executor.memory",
                 os.getenv("SPARK_EXECUTOR_MEMORY", "4g"))
        conf.set("spark.executor.cores",
                 os.getenv("SPARK_EXECUTOR_CORES", "2"))
        conf.set("spark.executor.instances",
                 os.getenv("SPARK_EXECUTOR_INSTANCES", "2"))
        conf.set("spark.driver.memory",
                 os.getenv("SPARK_DRIVER_MEMORY", "2g"))
    else:
        # Локальный режим (docker)
        conf.setMaster("local[2]")
        conf.set("spark.jars.packages", ICEBERG_PACKAGE)
        conf.set("spark.jars.ivy", "/tmp/ivy")
        conf.set("spark.sql.catalog.local",
                 "org.apache.iceberg.spark.SparkCatalog")
        conf.set("spark.sql.catalog.local.type", "hadoop")
        conf.set("spark.sql.catalog.local.warehouse", "/app/warehouse")

    return conf


def create_spark(app_name: str = "BigDataLab") -> SparkSession:
    conf = create_spark_configuration(app_name)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def catalog_name() -> str:
    """Имя Iceberg-каталога в текущем окружении."""
    return "spark_catalog" if _is_cluster() else "local"
