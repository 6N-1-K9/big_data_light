import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    regexp_extract_all,
    regexp_replace,
)


SELECTED_COLUMNS = [
    "vin",
    "body_type",
    "daysonmarket",
    "fleet",
    "has_accidents",
    "horsepower",
    "is_certified",
    "is_cpo",
    "is_oemcpo",
    "major_options",
    "maximum_seating",
    "mileage",
    "price",
    "wheel_system",
    "year",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Очистка CSV и сохранение результата в Apache Iceberg"
    )

    parser.add_argument(
        "input",
        help="Путь к исходному CSV",
    )

    parser.add_argument(
        "--table",
        default="local.lab1.used_cars",
        help="Имя Iceberg-таблицы",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Ограничить количество строк для тестового запуска",
    )

    return parser.parse_args()


def transform_dataframe(data: DataFrame) -> DataFrame:
    return (
        data
        .select(*SELECTED_COLUMNS)

        .withColumn(
            "daysonmarket",
            col("daysonmarket").cast("integer"),
        )

        .withColumn(
            "fleet",
            col("fleet").cast("boolean"),
        )

        .withColumn(
            "has_accidents",
            col("has_accidents").cast("boolean"),
        )

        .withColumn(
            "horsepower",
            col("horsepower").cast("float"),
        )

        .withColumn(
            "is_certified",
            col("is_certified").cast("boolean"),
        )

        .withColumn(
            "is_cpo",
            col("is_cpo").cast("boolean"),
        )

        .withColumn(
            "is_oemcpo",
            col("is_oemcpo").cast("boolean"),
        )

        .withColumn(
            "major_options",
            regexp_extract_all(
                col("major_options"),
                lit(r"'([^']*)'"),
                1,
            ),
        )

        .withColumn(
            "maximum_seating",
            regexp_replace(
                col("maximum_seating"),
                r"\s+seats",
                "",
            ).cast("integer"),
        )

        .withColumn(
            "mileage",
            col("mileage").cast("float"),
        )

        .withColumn(
            "price",
            col("price").cast("float"),
        )

        .withColumn(
            "year",
            col("year").cast("integer"),
        )
    )


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("Lab1DataCleansing")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"\nИсходный файл: {args.input}")
    print(f"Iceberg-таблица: {args.table}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(args.input)
    )

    if args.limit is not None:
        print(
            f"Тестовый режим: используются первые "
            f"{args.limit:,} строк"
        )

        df = df.limit(args.limit)

    df = transform_dataframe(df)

    print("\nСхема очищенного DataFrame:")
    df.printSchema()

    print("\nПример очищенных данных:")
    df.show(10, truncate=False)

    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS local.lab1"
    )

    print("\nСохраняем данные в Iceberg...")

    (
        df.writeTo(args.table)
        .using("iceberg")
        .createOrReplace()
    )

    print("Iceberg-таблица успешно сохранена.")

    print("\nПроверяем чтение сохранённой таблицы:")

    saved_df = spark.table(args.table)

    saved_df.show(10, truncate=False)

    print("\nСхема таблицы Iceberg:")
    saved_df.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
