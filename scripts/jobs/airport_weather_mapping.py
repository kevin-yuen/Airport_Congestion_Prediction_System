import scripts.ingestions.airport_weather_mapping_raw as m_ingestion
import scripts.transformations.airport_weather_mapping_cleaned as m_transformation

import pyspark.sql.functions as F


def run_airport_weather_mapping_job(
    spark,
    airport_cleaned_path: str,
    weather_cleaned_path: str,
    transformed_csv_path: str
):
    # ---------------------------------------------------
    # [START] INGESTION
    # ---------------------------------------------------
    airport_df, weather_df = m_ingestion.load_mapping_sources(
        spark, 
        airport_cleaned_path, 
        weather_cleaned_path
    )

    cross_df = m_ingestion.ingest_data(airport_df, weather_df)
    # ---------------------------------------------------
    # [END] INGESTION
    # ---------------------------------------------------


    # ---------------------------------------------------
    # [START] TRANSFORMATION
    # ---------------------------------------------------
    # calculate distance between airport and corresponding weather stations in miles
    cross_df = m_transformation.calculate_distance(cross_df)

    # only keep the nearest weather station
    mapping_df = m_transformation.get_nearest_station(cross_df)
    # ---------------------------------------------------
    # [END] TRANSFORMATION
    # ---------------------------------------------------


    # ---------------------------------------------------
    # [START] DQ CHECK AFTER TRANSFORMATION
    # ---------------------------------------------------
    duplicate_pk_count = (
        mapping_df
        .groupBy(
            "iata_code", "station_id"
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_pk_count > 0:
        raise ValueError(f"Duplicate composite PK count: {duplicate_pk_count}")

    print(f"[SUCCESS] Airport-weather mapping created.")

    print(f"[INFO] Total mappings: {mapping_df.count()}")

    mapping_df.printSchema()
    mapping_df.show(50)
    # ---------------------------------------------------
    # [END] DQ CHECK AFTER TRANSFORMATION
    # ---------------------------------------------------

    mapping_df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .csv(transformed_csv_path)
    print(f"[SUCCESS] Parquet written to: {transformed_csv_path}")

    return mapping_df
