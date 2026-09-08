from pyspark.sql import SparkSession
from pyspark.sql.functions import avg


def main():
    spark = (
        SparkSession.builder
        .appName("SparkTest")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"\nSpark version: {spark.version}\n")

    data = [
        ("BMW", 2018, 15000),
        ("Audi", 2020, 22000),
        ("Toyota", 2019, 18000),
        ("BMW", 2021, 27000),
    ]

    df = spark.createDataFrame(
        data,
        ["brand", "year", "price"],
    )

    print("Исходный DataFrame:")
    df.show()

    print("Средняя цена автомобилей по маркам:")

    df.groupBy("brand").agg(
        avg("price").alias("average_price")
    ).show()

    spark.stop()


if __name__ == "__main__":
    main()
