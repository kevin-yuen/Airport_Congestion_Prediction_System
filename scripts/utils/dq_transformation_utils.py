import pyspark.sql.functions as F


def check_column_uniqueness(df, columns):
    total_row_count = df.count()

    agg_exprs = [
        F.countDistinct(F.col(c)).alias(c) for c in columns
    ]
    distinct_counts = df.agg(*agg_exprs).collect()[0].asDict()

    result = {
        c: 'Yes' if distinct_counts[c] == total_row_count else 'No'
        for c in columns
    }

    output = "\n".join(f"{col}: {result[col]}" for col in columns)
    print("\nColumn Uniqueness:\n" + output)


def check_null_counts(df):
    cols = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    null_counts = df.select(cols).first().asDict()

    output = "\n".join(f"{col_name}: {count}" for col_name, count in null_counts.items())
    print(f"\nNull Counts:\n{output}\n")


def check_max_and_min(df, column):
    stats = df.select(F.max(column), F.min(column)).first()
    max_value = stats[0]
    min_value = stats[1]
    print(f"\nMax: {max_value}\nMin: {min_value}\n")


def check_row_counts(df, parquet_df, partition, affected_partitions, df_type):
    df_count = df.filter(
        df[partition].isin(affected_partitions)
    ).count()

    parquet_df_count = parquet_df.filter(
        parquet_df[partition].isin(affected_partitions)
    ).count()
    
    if df_count != parquet_df_count:
        raise ValueError(f"""
            Row count mismatch:\n
            {df_type} df: {df_count}\n
            {df_type} parquet df: {parquet_df_count}\n
        """)
    else:
        print(f"""[INFO] TSA Throughput {df_type} parquet dataset affected row count: 
              {parquet_df_count}""")
        

def check_distinct_row_count(df, columns):
    total_rows = df.count()

    distinct_rows = (
        df.select(
            F.count_distinct(*columns).alias("distinct_count")
        )
        .collect()[0]["distinct_count"]
    )

    print(f"Total rows: {total_rows}")
    print(f"Distinct combinations: {distinct_rows}")

