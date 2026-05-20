import scripts.utils.utils as u


def load_dependency_data(
    spark,
    flight_performance_cleaned_path,
    tsa_throughput_cleaned_path,
    tsa_wait_time_cleaned_path,
    weather_cleaned_path,
    airport_weather_mapping_path
):
    # validate dependency datasets exist
    required_paths = [
        flight_performance_cleaned_path,
        tsa_throughput_cleaned_path,
        tsa_wait_time_cleaned_path,
        weather_cleaned_path,
        airport_weather_mapping_path
    ]

    for path in required_paths:
        if not u.path_exists(spark, path):
            raise FileNotFoundError(f"Required dataset not found: {path}")

    print("[INFO] All dependency datasets found.")

    # load datasets
    flight_df = spark.read.parquet(flight_performance_cleaned_path)
    throughput_df = spark.read.parquet(tsa_throughput_cleaned_path)
    wait_time_df = spark.read.parquet(tsa_wait_time_cleaned_path)
    weather_df = spark.read.parquet(weather_cleaned_path)
    mapping_df = spark.read.csv(airport_weather_mapping_path, header=True, inferSchema=True)

    print("[INFO] All datasets loaded successfully.")

    return (
        flight_df,
        throughput_df,
        wait_time_df,
        weather_df,
        mapping_df
    )
