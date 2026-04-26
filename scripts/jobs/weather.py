import pandas as pd
from config import ENV, CONFIG
import scripts.utils.utils as u
from pyspark.sql.types import StructType, StructField, StringType


def normalize_rows(row, expected_length):
    if len(row) < expected_length:
        return row + [None] * (expected_length - len(row))
    elif len(row) > expected_length:
        return row[:expected_length]
    return row


def process_csv(file_path, expected_length):
    try:
        pd_df = pd.read_csv(file_path)

        pd_df = pd_df.astype(str)

        for row in pd_df.itertuples(index=False, name=None):
            yield normalize_rows(list(row), expected_length)
    except Exception as e:
        filename = file_path.split("/")[-1]
        print(f"Error processing CSV file {filename}: {e}")


def create_schema_from_sample(df):
    return StructType([StructField(col, StringType(), True) for col in df.columns])


def load_weather_dataframe():
    base_path = CONFIG[ENV]["weather"]
    spark = u.get_spark()

    csv_files = u.get_files_by_state(base_path)
    if not csv_files:
        raise ValueError("No weather CSV files found")

    # get schema from a sample file to ensure correct column names and types
    sample_df = pd.read_csv(csv_files[0], nrows=0)
    schema = create_schema_from_sample(sample_df)

    expected_length = len(schema.fields)

    csv_rows_rdd = u.parallelize_processing(
        csv_files, 
        lambda file: process_csv(file, expected_length)
    )
    weather_raw = spark.createDataFrame(csv_rows_rdd, schema=schema)

    return weather_raw


weather_raw_df = load_weather_dataframe()
weather_raw_df.show(5)

print(f"Total rows: {weather_raw_df.count()}")
