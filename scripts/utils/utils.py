from scripts.utils.spark_session import get_spark
from functools import reduce
import os


def load_raw_data(root_path, filename=None, header=True):
    spark = get_spark()

    if filename is not None:
        root_path = root_path + filename

    df = spark.read \
        .option("header", header) \
        .option("inferSchema", "true") \
        .csv(root_path)
    
    return df


# def load_partitioned_data_by_year(base_path, start_year, end_year, file_formats):
#     dfs = []

#     for year in range(start_year, end_year+1):
#         folder = os.path.join(base_path, f"year={year}")

#         for file in os.listdir(folder):
#             if file.endswith(tuple(file_formats)):
#                 df = load_raw_data(folder + "/", file)
#                 dfs.append(df)

#     return reduce(lambda a, b: a.unionByName(b), dfs)
