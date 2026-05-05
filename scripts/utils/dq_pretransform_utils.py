import pyspark.sql.functions as F


def is_column_unique(df):
    is_unique = {}
    total_row_count = df.count()

    for column in df.columns:
        distinct_count = df.select(column).distinct().count()
        is_unique[column] = 'Y' if total_row_count == distinct_count else 'N'

    output = "\n".join(f"{column}: {uniqueness}" for column, uniqueness in is_unique.items())
    print("\nColumn Uniqueness:\n" + output)


def count_nulls(df):
    cols = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    null_counts = df.select(cols).first().asDict()

    output = "\n".join(f"{col_name}: {count}" for col_name, count in null_counts.items())
    print(f"\nNull Counts:\n{output}\n")


def get_max_and_min(df, column):
    stats = df.select(F.max(column), F.min(column)).first()
    max_value = stats[0]
    min_value = stats[1]
    print(f"\nmax: {max_value}\nmin: {min_value}\n")
