from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    desc,
    isnan,
    sum as spark_sum,
    when,
)


TABLE = "local.lab1.used_cars"
OUTPUT_DIR = Path("/app/output/results")


def main():
    spark = (
        SparkSession.builder
        .appName("Lab1ExploratoryAnalysis")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\nЧитаем Iceberg-таблицу: {TABLE}")

    df = spark.table(TABLE)

    row_count = df.count()
    column_count = len(df.columns)

    print(f"\nКоличество строк: {row_count:,}")
    print(f"Количество столбцов: {column_count}")

    print("\nСхема:")
    df.printSchema()

    # --------------------------------------------------
    # Пропуски
    # --------------------------------------------------

    print("\nАнализ пропущенных значений...")

    missing_expressions = []

    for field in df.schema.fields:
        column = field.name

        if field.dataType.simpleString() in (
            "float",
            "double",
        ):
            condition = (
                col(column).isNull()
                | isnan(col(column))
            )
        else:
            condition = col(column).isNull()

        missing_expressions.append(
            spark_sum(
                when(condition, 1).otherwise(0)
            ).alias(column)
        )

    missing_row = (
        df.select(*missing_expressions)
        .collect()[0]
        .asDict()
    )

    missing_data = []

    for column, missing_count in missing_row.items():
        missing_count = int(missing_count or 0)

        missing_percent = (
            missing_count / row_count * 100
            if row_count
            else 0
        )

        missing_data.append(
            {
                "column": column,
                "missing_count": missing_count,
                "missing_percent": round(
                    missing_percent,
                    2,
                ),
            }
        )

    missing_df = pd.DataFrame(missing_data)

    missing_df = missing_df.sort_values(
        "missing_percent",
        ascending=False,
    )

    print("\nПропуски:")
    print(
        missing_df.to_string(
            index=False
        )
    )

    missing_df.to_csv(
        OUTPUT_DIR / "missing_values.csv",
        index=False,
    )

    # --------------------------------------------------
    # Числовые статистики
    # --------------------------------------------------

    numeric_columns = [
        "daysonmarket",
        "horsepower",
        "maximum_seating",
        "mileage",
        "price",
        "year",
    ]

    print("\nОписательная статистика:")

    stats = (
        df.select(*numeric_columns)
        .describe()
    )

    stats.show(
        truncate=False
    )

    stats.toPandas().to_csv(
        OUTPUT_DIR / "numeric_statistics.csv",
        index=False,
    )

    # --------------------------------------------------
    # Категориальные признаки
    # --------------------------------------------------

    categorical_columns = [
        "body_type",
        "wheel_system",
    ]

    for column in categorical_columns:
        print(
            f"\nРаспределение '{column}':"
        )

        result = (
            df.groupBy(column)
            .agg(
                count("*").alias("count")
            )
            .orderBy(desc("count"))
        )

        result.show(
            30,
            truncate=False,
        )

        result.toPandas().to_csv(
            OUTPUT_DIR
            / f"{column}_distribution.csv",
            index=False,
        )

    print(
        "\nРезультаты сохранены в "
        "/app/output/results"
    )

    spark.stop()


if __name__ == "__main__":
    main()
