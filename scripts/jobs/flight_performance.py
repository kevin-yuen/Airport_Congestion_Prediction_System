import pyspark.sql.functions as F
from config import ENV, CONFIG
import scripts.utils.utils as u
from functools import reduce
import os


def load_raw_flight_performance():
    base_path = CONFIG[ENV]["flight_performance"]

    dfs = []

    for year in range(2013, 2016):
        folder = os.path.join(base_path, f"year={year}")

        for file in os.listdir(folder):
            if file.endswith(".csv"):
                df = u.load_raw_data(folder + "/", file)
                dfs.append(df)

    return reduce(lambda a, b: a.unionByName(b), dfs)


flight_performance_raw_df = load_raw_flight_performance()
flight_performance_raw_df.show(5)
