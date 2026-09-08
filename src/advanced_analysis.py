from itertools import combinations
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    floor,
    max as spark_max,
    min as spark_min,
)


TABLE = "local.lab1.used_cars"

RESULTS_DIR = Path("/app/output/results")
PLOTS_DIR = Path("/app/output/plots")

NUMERIC_COLUMNS = [
    "daysonmarket",
    "horsepower",
    "maximum_seating",
    "mileage",
    "price",
    "year",
]


def calculate_outliers(df):
    rows = []

    for column in NUMERIC_COLUMNS:
        clean = df.select(column).where(
            col(column).isNotNull()
        )

        q1, median, q3 = clean.approxQuantile(
            column,
            [0.25, 0.5, 0.75],
            0.01,
        )

        iqr = q3 - q1

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        lower_count = clean.where(
            col(column) < lower_bound
        ).count()

        upper_count = clean.where(
            col(column) > upper_bound
        ).count()

        total_count = clean.count()

        rows.append(
            {
                "column": column,
                "q1": q1,
                "median": median,
                "q3": q3,
                "iqr": iqr,
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
                "lower_outliers": lower_count,
                "upper_outliers": upper_count,
                "total_outliers": lower_count + upper_count,
                "valid_values": total_count,
            }
        )

    return pd.DataFrame(rows)


def create_boxplots(outliers_df):
    for _, row in outliers_df.iterrows():
        column = row["column"]

        stats = [
            {
                "label": column,
                "whislo": row["lower_bound"],
                "q1": row["q1"],
                "med": row["median"],
                "q3": row["q3"],
                "whishi": row["upper_bound"],
                "fliers": [],
            }
        ]

        fig, ax = plt.subplots(figsize=(8, 4))

        ax.bxp(
            stats,
            vert=False,
        )

        ax.set_title(f"Boxplot: {column}")
        ax.set_xlabel(column)

        fig.tight_layout()

        fig.savefig(
            PLOTS_DIR / f"boxplot_{column}.png"
        )

        plt.close(fig)


def create_histogram(df, column, bins=30):
    bounds = (
        df.select(column)
        .where(col(column).isNotNull())
        .agg(
            spark_min(column).alias("min"),
            spark_max(column).alias("max"),
        )
        .collect()[0]
    )

    min_value = bounds["min"]
    max_value = bounds["max"]

    if min_value is None or max_value is None:
        return

    if min_value == max_value:
        return

    width = (max_value - min_value) / bins

    histogram = (
        df.select(column)
        .where(col(column).isNotNull())
        .withColumn(
            "bucket",
            floor(
                (col(column) - min_value) / width
            ),
        )
        .where(col("bucket") < bins)
        .groupBy("bucket")
        .agg(count("*").alias("count"))
        .orderBy("bucket")
    )

    pandas_hist = histogram.toPandas()

    pandas_hist["start"] = (
        min_value
        + pandas_hist["bucket"] * width
    )

    pandas_hist["end"] = (
        pandas_hist["start"] + width
    )

    pandas_hist.to_csv(
        RESULTS_DIR
        / f"histogram_{column}.csv",
        index=False,
    )

    plt.figure(figsize=(9, 5))

    plt.bar(
        pandas_hist["start"],
        pandas_hist["count"],
        width=width,
        align="edge",
    )

    plt.xlabel(column)
    plt.ylabel("Количество")
    plt.title(f"Распределение: {column}")
    plt.tight_layout()

    plt.savefig(
        PLOTS_DIR
        / f"histogram_{column}.png"
    )

    plt.close()


def calculate_correlations(df):
    matrix = pd.DataFrame(
        index=NUMERIC_COLUMNS,
        columns=NUMERIC_COLUMNS,
        dtype=float,
    )

    for column in NUMERIC_COLUMNS:
        matrix.loc[column, column] = 1.0

    for first, second in combinations(
        NUMERIC_COLUMNS,
        2,
    ):
        pair = (
            df.select(first, second)
            .where(
                col(first).isNotNull()
                & col(second).isNotNull()
            )
        )

        correlation = pair.stat.corr(
            first,
            second,
        )

        matrix.loc[first, second] = correlation
        matrix.loc[second, first] = correlation

    return matrix


def main():
    spark = (
        SparkSession.builder
        .appName("Lab1AdvancedEDA")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    RESULTS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    PLOTS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print(
        f"\nЧитаем Iceberg-таблицу: {TABLE}"
    )

    df = spark.table(TABLE)

    print("\nВычисляем квантили и выбросы...")

    outliers_df = calculate_outliers(df)

    print(
        outliers_df.to_string(
            index=False
        )
    )

    outliers_df.to_csv(
        RESULTS_DIR / "outliers_iqr.csv",
        index=False,
    )

    print("\nСоздаём boxplot-графики...")

    create_boxplots(outliers_df)

    print("\nСтроим распределения...")

    for column in NUMERIC_COLUMNS:
        print(f"  {column}")

        create_histogram(
            df,
            column,
        )

    print("\nВычисляем корреляции...")

    correlations = calculate_correlations(df)

    print("\nКорреляционная матрица:")
    print(correlations)

    correlations.to_csv(
        RESULTS_DIR
        / "correlation_matrix.csv"
    )

    plt.figure(figsize=(9, 7))

    sns.heatmap(
        correlations.astype(float),
        annot=True,
        fmt=".2f",
        square=True,
    )

    plt.title(
        "Корреляционная матрица"
    )

    plt.tight_layout()

    plt.savefig(
        PLOTS_DIR
        / "correlation_matrix.png"
    )

    plt.close()

    print(
        "\nГотово.\n"
        "Таблицы: /app/output/results\n"
        "Графики: /app/output/plots"
    )

    spark.stop()


if __name__ == "__main__":
    main()
