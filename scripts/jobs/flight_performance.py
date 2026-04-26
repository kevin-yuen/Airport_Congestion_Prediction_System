import pyspark.sql.functions as F
from config import ENV, CONFIG
import scripts.utils.utils as u
from functools import reduce


def load_raw_flight_performance():
    base_path = CONFIG[ENV]["flight_performance"]

    _, _, csv_files =u.get_files_by_year(base_path, 2013, 2015)

    dfs = [u.load_raw_data(f) for f in csv_files]
    return reduce(lambda a, b: a.unionByName(b), dfs)


flight_performance_raw = load_raw_flight_performance()
flight_performance_raw.show(5)
