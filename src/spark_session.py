from pyspark.sql import SparkSession


ICEBERG_PACKAGE = (
    "org.apache.iceberg:"
    "iceberg-spark-runtime-3.5_2.12:1.11.0"
)


def create_spark(app_name="BigDataLab"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config(
            "spark.jars.packages",
            ICEBERG_PACKAGE,
        )
        .config(
            "spark.jars.ivy",
            "/tmp/ivy",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            "spark.sql.catalog.local.type",
            "hadoop",
        )
        .config(
            "spark.sql.catalog.local.warehouse",
            "/app/warehouse",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
