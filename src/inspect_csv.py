import csv
import os
import sys


def shorten(value, limit=80):
    value = value.replace("\n", "\\n")
    if len(value) > limit:
        return value[:limit] + "..."
    return value


def main():
    if len(sys.argv) != 2:
        print("Использование: python3 inspect_csv.py <csv-файл>")
        sys.exit(1)

    path = sys.argv[1]

    size_gb = os.path.getsize(path) / (1024 ** 3)

    print(f"Файл: {path}")
    print(f"Размер: {size_gb:.2f} GiB")

    with open(
        path,
        "r",
        encoding="utf-8-sig",
        errors="replace",
        newline=""
    ) as file:
        sample = file.read(65536)
        file.seek(0)

        try:
            dialect = csv.Sniffer().sniff(
                sample,
                delimiters=",;\t|"
            )
            delimiter = dialect.delimiter
        except csv.Error:
            delimiter = ","

        reader = csv.reader(file, delimiter=delimiter)

        header = next(reader)

        print(f"Разделитель: {repr(delimiter)}")
        print(f"Количество столбцов: {len(header)}")

        print("\nСтолбцы:")
        for i, column in enumerate(header, start=1):
            print(f"{i:2}. {column}")

        print("\nПервые 3 записи:")

        for row_number in range(1, 4):
            try:
                row = next(reader)
            except StopIteration:
                break

            print(
                f"\n--- Запись {row_number} "
                f"({len(row)} столбцов) ---"
            )

            for column, value in zip(header, row):
                print(f"{column}: {shorten(value)}")


if __name__ == "__main__":
    main()
