import pyspark.sql.functions as F
import scripts.utils.utils as u
import glob
from functools import reduce

    
def load_departure_performance_data(incoming_path, spark):
    dfs = []
    new_columns = ['rank', 'airport_location', 'performance (%)']
    csv_files = glob.glob(f"{incoming_path}Airport_OnTime_Departure_Performance_*.csv")

    if not csv_files:
        raise FileNotFoundError(f"No departure performance files found in: {incoming_path}")

    for csv_file_path in csv_files:
        temp_df = u.load_raw_data(csv_file_path, spark)
        temp_df = temp_df.toDF(*new_columns)

        file_year = csv_file_path.split('.')[0][-4:]

        # mark the year that the current data load corresponds to
        temp_df = temp_df.withColumn('year', F.lit(file_year))

        dfs.append(temp_df)
    
    raw_df = reduce(lambda df1, df2: df1.unionByName(df2), dfs)
    
    return csv_files, raw_df
