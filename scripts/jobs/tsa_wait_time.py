from config import ENV, CONFIG
import scripts.utils.utils as u
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType


def create_schema():
    return [StructField(f'col_{i}', StringType(), True) for i in range(1, 127)]


def load_raw_data(root_path, filename=None, header=True):
    spark = u.get_spark()

    if filename is not None:
        root_path = root_path + filename
    
    pd_df = pd.read_excel(root_path, skiprows=3, header=None, engine='xlrd')

    schema = [StructField('iata_code', StringType(), True), \
                StructField('airport', StringType(), True), \
                StructField('checkpoint', StringType(), True)]
    schema += create_schema()

    pd_df.columns = [field.name for field in schema]

    raw_df = spark.createDataFrame(pd_df, schema=StructType(schema))
    return raw_df


root_path = CONFIG[ENV]["tsa_wait_time"]
tsa_wait_time_raw = load_raw_data(root_path, 'tsa-wait-times-january-2006-december-2015.xls')

tsa_wait_time_raw.select(['iata_code', 'airport', 'checkpoint', 'col_1', 'col_11']).show(5)
