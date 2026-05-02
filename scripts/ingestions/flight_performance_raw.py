import scripts.utils.utils as u
from functools import reduce


def load_flight_performance_data(root_path, spark):
    _, _, csv_files = u.get_files_by_year(root_path, 2013, 2015)
    dfs = [u.load_raw_data(file_path=f, spark=spark) for f in csv_files]

    raw_df = reduce(lambda a, b: a.unionByName(b), dfs)

    return csv_files, raw_df

