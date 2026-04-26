from scripts.utils.spark_session import get_spark
from functools import reduce
import os
import constants as c


spark = get_spark()

def load_raw_data(root_path, filename=None, header=True):
    if filename is not None:
        root_path = root_path + filename

    df = spark.read \
        .option("header", header) \
        .option("inferSchema", "true") \
        .csv(root_path)
    
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

    for k, _ in c.states.items():
        folder = os.path.join(base_path, f"state={k}")

        for file in os.listdir(folder):
            if file.endswith(".csv"):
                csv_files.append(os.path.join(folder, file))

    return csv_files


def parallelize_processing(files, process_function):
    num_cores = spark.sparkContext.defaultParallelism       # get the number of available cores
    num_partitions = num_cores * 2                          # set number of partitions to 2x the number of cores

    rdd = spark.sparkContext.parallelize(files, num_partitions)
    rows_rdd = rdd.flatMap(process_function)

    return rows_rdd
