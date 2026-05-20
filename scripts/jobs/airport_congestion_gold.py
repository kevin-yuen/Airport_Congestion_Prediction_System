import pyspark.sql.functions as F

import scripts.utils.dq_transformation_utils as dq
import scripts.ingestions.airport_congestion_gold_raw as gold_ingestion
import scripts.transformations.airport_congestion_gold_cleaned as gold_transformation


def run_airport_congestion_gold_job(
    spark,
    flight_performance_cleaned_path,
    tsa_throughput_cleaned_path,
    tsa_wait_time_cleaned_path,
    weather_cleaned_path,
    airport_weather_mapping_path,
    gold_output_path
):
    # ---------------------------------------------------
    # [START] INGESTION
    # ---------------------------------------------------
    (
        flight_df,
        throughput_df, 
        wait_time_df, 
        weather_df, 
        mapping_df
    ) = gold_ingestion.load_dependency_data(
        spark,
        flight_performance_cleaned_path,
        tsa_throughput_cleaned_path,
        tsa_wait_time_cleaned_path,
        weather_cleaned_path,
        airport_weather_mapping_path
    )
     # ---------------------------------------------------
    # [END] INGESTION
    # ---------------------------------------------------


    # ===================================================
    # [START] DQ CHECK BEFORE TRANSFORMATION
    # ===================================================
    print("""
***********************************************
*********** DQ BEFORE TRANSFORMATION ***********
************************************************
""")

    datasets = {
        "flight": flight_df,
        "throughput": throughput_df,
        "wait_time": wait_time_df,
        "weather": weather_df,
        "mapping": mapping_df
    }

    for dataset_name, df in datasets.items():
        print(f"\n========== {dataset_name.upper()} ==========\n")
        print(f"Row count: {df.count()}")

        dq.check_null_counts(df)
        df.printSchema()
    # ===================================================
    # [END] DQ CHECK BEFORE TRANSFORMATION
    # ===================================================


    # ===================================================
    # [START] TRANSFORMATION
    # ===================================================
    # aggregate flight performance
    flight_agg_df = gold_transformation.aggregate_flight_performance(flight_df)
    
    # calculate total taxi time
    flight_agg_df = gold_transformation.calculate_total_taxi_time(flight_agg_df)

    # aggregate TSA throughput
    throughput_agg_df = gold_transformation.aggregate_throughput(throughput_df)

    # aggregate TSA wait time
    wait_time_agg_df = gold_transformation.aggregate_wait_time(wait_time_df)

    # join weather and airport_weather_mapping
    airport_weather_df = gold_transformation.join_weather(mapping_df, weather_df)
    
    # create gold dataset
    gold_df = gold_transformation.create_gold_dataset(
        flight_agg_df,
        throughput_agg_df,
        wait_time_agg_df,
        airport_weather_df
    )

    # calculate passenger-to-flight ratio
    gold_df = gold_transformation.calculate_passenger_to_flight_ratio(gold_df)

    # round numeric columns
    numeric_columns = [
        "avg_departure_delay",
        "avg_arrival_delay",
        "delay_rate",
        "cancellation_rate",
        "avg_carrier_delay",
        "avg_weather_delay",
        "avg_nas_delay",
        "avg_security_delay",
        "avg_late_aircraft_delay",
        "avg_taxi_out_time",
        "avg_taxi_in_time",
        "avg_total_taxi_time",
        "avg_wait_time_minutes",
        "passenger_to_flight_ratio",
        "avg_temperature",
        "total_precipitation",
        "total_snowfall"
    ]
    gold_df = gold_transformation.round_numeric_columns(gold_df, numeric_columns)
    # ===================================================
    # [END] TRANSFORMATION
    # ===================================================


    # ===================================================
    # [START] DQ CHECK AFTER TRANSFORMATION
    # ===================================================
    print("""
***********************************************
*********** DQ AFTER TRANSFORMATION ***********
************************************************
""")
    # schema check
    gold_df.printSchema()

    # null check
    dq.check_null_counts(gold_df)

    # duplicate PK check
    duplicate_pk_count = gold_df.groupBy(
            "iata_code",
            "year",
            "month"
        ).count().filter(F.col("count") > 1).count()
    
    print(f"Duplicate PK count: {duplicate_pk_count}" )

    # sample data
    gold_df.show(20, truncate=False)

    # row count check
    print(f"Row count: {gold_df.count()}")
    # ===================================================
    # [END] DQ CHECK AFTER TRANSFORMATION
    # ===================================================


    gold_df.write.mode("overwrite").partitionBy("year").parquet(gold_output_path)
    print(f"[SUCCESS] airport_congestion_gold written to: {gold_output_path}")
