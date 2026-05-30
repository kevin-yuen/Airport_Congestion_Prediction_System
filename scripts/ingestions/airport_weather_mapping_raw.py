import pyspark.sql.functions as F
import glob
import os

import scripts.utils.utils as u


def load_mapping_sources(
    spark,
    airport_cleaned_path: str,
    weather_cleaned_path: str
):
    # airport_cleaned_path = glob.glob(os.path.join(airport_cleaned_path, "*.csv"))

    # validate airport dataset exists
    if not u.path_exists(spark, airport_cleaned_path):
        raise FileNotFoundError(
            f"Airport cleaned dataset not found:{airport_cleaned_path}"
        )

    # validate weather dataset exists
    if not u.path_exists(spark, weather_cleaned_path):
        raise FileNotFoundError(
            f"Weather cleaned dataset not found: {weather_cleaned_path}"
        )

    airport_df = spark.read.option("header", True).csv(airport_cleaned_path)
    weather_df = spark.read.parquet(weather_cleaned_path)

    # ensure datasets are not empty
    if airport_df.rdd.isEmpty():
        raise ValueError("Airport cleaned dataset is empty.")

    if weather_df.rdd.isEmpty():
        raise ValueError("Weather cleaned dataset is empty.")

    return airport_df, weather_df


def ingest_data(
    airport_df,
    weather_df
):
    airport_selected_df = (
        airport_df
        .select(
            F.col("iata_code"),
            F.col("latitude").cast("double").alias("airport_lat"),
            F.col("longitude").cast("double").alias("airport_lon")
        )
        .dropna(
            subset=[
                "iata_code",
                "airport_lat",
                "airport_lon"
            ]
        )
        .dropDuplicates()
    )

    weather_selected_df = (
        weather_df
        .select(
            F.col("station_id"),
            F.col("latitude").cast("double").alias("station_lat"),
            F.col("longitude").cast("double").alias("station_lon")
        )
        .dropna(
            subset=[
                "station_id",
                "station_lat",
                "station_lon"
            ]
        )
        .dropDuplicates()
    )

    cross_df = airport_selected_df.crossJoin(weather_selected_df)
    return cross_df
