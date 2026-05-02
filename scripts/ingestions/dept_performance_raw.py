import pyspark.sql.functions as F
import scripts.utils.utils as u
from functools import reduce

    
def load_departure_performance_data(root_path, spark):
    start_year = 2003
    end_year = 2024

    csv_files = []
    dfs = []
    new_columns = ['rank', 'airport_location', 'performance (%)']

    # load raw data by year (2003 - 2024)
    for i in range(start_year, end_year+1):
        file_name = f"Airport_OnTime_Departure_Performance_0101{i}.csv"
        file_path = root_path + file_name

        temp_df = u.load_raw_data(file_path, spark)
        temp_df = temp_df.toDF(*new_columns)

        # mark the year that the current data load corresponds to
        temp_df = temp_df.withColumn('year', F.lit(i))

        csv_files.append(file_path)
        dfs.append(temp_df)

    raw_df = reduce(lambda df1, df2: df1.unionByName(df2), dfs)
    
    return csv_files, raw_df
