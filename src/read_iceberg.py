from pyspark.sql import SparkSession


TABLE = "local.lab1.used_cars"


def main():
    spark = (
        SparkSession.builder
        .appName("ReadIceberg")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"\nЧитаем Iceberg-таблицу: {TABLE}\n")

    df = spark.table(TABLE)

    df.printSchema()

    print("\nПример данных:")
    df.show(10, truncate=False)

    print("\nТаблицы namespace lab1:")
    spark.sql(
        "SHOW TABLES IN local.lab1"
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
