import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType


def verify_wait_time_row_counts(pd_df, spark_df):
    pd_df_count = len(pd_df)
    spark_df_count = spark_df.count()

    if pd_df_count == spark_df_count:
        print(f"""Row count check passed for TSA wait time data!\nsource file: {pd_df_count}\nraw dataset: {spark_df_count}\n""")
    else:
        raise ValueError(f"""Row count check failed for TSA wait time data!\nsource file: ({pd_df_count})\nraw dataset: ({spark_df_count})""")


def __build_dynamic_columns_schema():
    return [StructField(f'col_{i}', StringType(), True) for i in range(1, 127)]


def load_tsa_wait_time_data(root_path, spark):
    file_path = root_path + 'tsa-wait-times-january-2006-december-2015.xls'
    
    pd_df = pd.read_excel(file_path, skiprows=3, header=None, engine='xlrd')

    schema = [StructField('iata_code', StringType(), True), \
                StructField('airport', StringType(), True), \
                StructField('checkpoint', StringType(), True)]
    schema += __build_dynamic_columns_schema()

    pd_df.columns = [field.name for field in schema]
    raw_df = spark.createDataFrame(pd_df, schema=StructType(schema))

    return pd_df, raw_df
