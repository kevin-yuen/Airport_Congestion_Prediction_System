import pyspark.sql.functions as F
from config import ENV, CONFIG
import scripts.utils.utils as u
from functools import reduce


def load_raw_dept_performance():
    input_path = CONFIG[ENV]["airport_dept_performance"]
    start_year = 2003
    end_year = 2024

    dfs = []
    new_columns = ['rank', 'airport_location', 'performance (%)']

    # Load raw data by year (2003 - 2024)
    for i in range(start_year, end_year+1):
        temp_df = u.load_raw_data(input_path, f"Airport_OnTime_Departure_Performance_0101{i}.csv")
        temp_df = temp_df.toDF(*new_columns)

        # Mark the year that the current data load corresponds to
        temp_df = temp_df.withColumn('year', F.lit(i))

        dfs.append(temp_df)

    raw_df = reduce(lambda df1, df2: df1.unionByName(df2), dfs)

    # Remove header-as-row issue
    raw_df = raw_df.filter(raw_df['rank'] != 'Rank')
    
    return raw_df


dept_performance_raw_df = load_raw_dept_performance()
dept_performance_raw_df.show(5)
