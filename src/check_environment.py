from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg


def main():
    spark = (
        SparkSession.builder
        .appName("EnvironmentCheck")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"\nSpark: {spark.version}")
    print(f"Pandas: {pd.__version__}")
    print(f"Seaborn: {sns.__version__}")

    data = [
        ("BMW", 15000),
        ("Audi", 22000),
        ("Toyota", 18000),
        ("BMW", 27000),
    ]

    df = spark.createDataFrame(data, ["brand", "price"])

    result = (
        df.groupBy("brand")
        .agg(avg("price").alias("average_price"))
        .orderBy("brand")
    )

    print("\nРезультат Spark:")
    result.show()

    pandas_df = result.toPandas()

    output_dir = Path("/app/output/plots")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "environment_test.png"

    plt.figure(figsize=(8, 5))
    plt.bar(pandas_df["brand"], pandas_df["average_price"])
    plt.xlabel("Brand")
    plt.ylabel("Average price")
    plt.title("Spark environment test")
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()

    print(f"График сохранён: {output_file}")

    spark.stop()


if __name__ == "__main__":
    main()
