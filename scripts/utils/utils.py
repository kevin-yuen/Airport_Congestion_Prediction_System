import os
import constants as C
import pyspark.sql.functions as F


def load_raw_data(file_path, spark, header=True):
    df = spark.read \
        .option("header", header) \
        .option("inferSchema", "true") \
        .csv(file_path)
    
    return df


def get_files_by_year(base_path, start_year, end_year):
    pdf_files = []
    excel_files = []
    csv_files = []

    for year in range(start_year, end_year+1):
        folder = os.path.join(base_path, f"year={year}")

        for file in os.listdir(folder):
            if file.endswith(".pdf"):
                pdf_files.append(os.path.join(folder, file))
            elif file.endswith(".xlsx"):
                excel_files.append(os.path.join(folder, file))
            elif file.endswith(".csv"):
                csv_files.append(os.path.join(folder, file))

    return pdf_files, excel_files, csv_files


def get_files_by_state(base_path):
    csv_files = []

    for k, _ in C.STATES.items():
        folder = os.path.join(base_path, f"state={k}")

        for file in os.listdir(folder):
            if file.endswith(".csv"):
                csv_files.append(os.path.join(folder, file))

    return csv_files


def parallelize_processing(files, process_function, spark):
    num_cores = spark.sparkContext.defaultParallelism       # get the number of available cores
    num_partitions = num_cores * 2                          # set number of partitions to 2x the number of cores

    rdd = spark.sparkContext.parallelize(files, num_partitions)
    rows_rdd = rdd.flatMap(process_function)
    return rows_rdd


def rename_columns(df, col_mapping):
    return df.withColumnsRenamed(col_mapping)


def cast_column_type(df, cast_columns, cast_type):
    for col in cast_columns:
        df = df.withColumn(col, F.col(col).cast(cast_type))

    return df
