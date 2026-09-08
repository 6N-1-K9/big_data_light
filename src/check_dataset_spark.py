import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    if len(sys.argv) != 2:
        print("Использование: check_dataset_spark.py <csv-файл>")
        sys.exit(1)

    input_path = sys.argv[1]

    spark = (
        SparkSession.builder
        .appName("DatasetCheck")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"\nЧитаем: {input_path}")

    # На первом этапе намеренно НЕ используем inferSchema.
    # Spark прочитает все поля как строки, не сканируя весь файл
    # ради автоматического определения типов.
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(input_path)
    )

    print(f"\nКоличество столбцов: {len(df.columns)}")
    print(f"Количество партиций: {df.rdd.getNumPartitions()}")

    print("\nСхема после первоначального чтения:")
    df.printSchema()

    selected_columns = [
        "make_name",
        "model_name",
        "year",
        "mileage",
        "price",
        "horsepower",
        "fuel_type",
        "transmission",
        "body_type",
        "listed_date",
        "seller_rating",
        "has_accidents",
    ]

    print("\nНесколько исходных записей:")

    df.select(*selected_columns).limit(5).show(
        truncate=False
    )

    print("\nПроверяем явное преобразование нескольких типов:")

    converted = df.select(
        col("make_name"),
        col("model_name"),
        col("year").cast("int").alias("year"),
        col("mileage").cast("double").alias("mileage"),
        col("price").cast("double").alias("price"),
        col("horsepower").cast("double").alias("horsepower"),
        col("seller_rating").cast("double").alias("seller_rating"),
    )

    converted.limit(5).show(truncate=False)

    print("\nСхема после явного преобразования:")
    converted.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
