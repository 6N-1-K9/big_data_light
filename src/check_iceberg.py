from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("IcebergCheck")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("\nСоздаём namespace lab1...")

    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS local.lab1
    """)

    print("Создаём тестовую Iceberg-таблицу...")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.lab1.test_cars (
            id BIGINT,
            brand STRING,
            year INT,
            price DOUBLE
        )
        USING iceberg
    """)

    spark.sql("""
        INSERT INTO local.lab1.test_cars VALUES
            (1, 'BMW', 2018, 15000.0),
            (2, 'Audi', 2020, 22000.0),
            (3, 'Toyota', 2019, 18000.0)
    """)

    print("\nСодержимое Iceberg-таблицы:")

    spark.sql("""
        SELECT *
        FROM local.lab1.test_cars
        ORDER BY id
    """).show()

    print("\nТаблицы в namespace lab1:")

    spark.sql("""
        SHOW TABLES IN local.lab1
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
