import pyspark.sql.functions as F


def aggregate_flight_performance(df):
    flight_agg_df = (
        df.groupBy(
            "origin_iata_code",
            "year",
            "month"
        ).agg(
            F.sum("flight_count").alias("total_flights"),
            F.avg("departure_delay").alias("avg_departure_delay"),
            F.avg("arrival_delay").alias("avg_arrival_delay"),
            F.avg(F.col("departure_delay_15min_flag").cast("double")).alias("delay_rate"),
            F.avg(F.col("is_cancelled").cast("double")).alias("cancellation_rate"),
            F.avg("carrier_delay").alias("avg_carrier_delay"),
            F.avg("weather_delay").alias("avg_weather_delay"),
            F.avg("nas_delay").alias("avg_nas_delay"),
            F.avg("security_delay").alias("avg_security_delay"),
            F.avg("late_aircraft_delay").alias("avg_late_aircraft_delay"),
            F.avg("taxi_out_time").alias("avg_taxi_out_time"),
            F.avg("taxi_in_time").alias("avg_taxi_in_time")
        ).withColumnRenamed("origin_iata_code", "iata_code")
    )

    return flight_agg_df


def calculate_total_taxi_time(df):
    df = df.withColumn(
        "avg_total_taxi_time",
        F.col("avg_taxi_out_time") + F.col("avg_taxi_in_time")
    )
    
    return df


def aggregate_throughput(df):
    throughput_agg_df = df.withColumn(
    "month",
        F.month("date")
    ).groupBy(
        "iata_code",
        "year",
        "month"
    ).agg(F.sum("passenger_count").alias("total_passenger_count"))

    return throughput_agg_df


def aggregate_wait_time(df):
    wait_time_agg_df = df.groupBy(
        "iata_code",
        "year",
        "month"
    ).agg(F.avg("avg_wait_time_minutes").alias("avg_wait_time_minutes"))

    return wait_time_agg_df


def join_weather(mapping_df, weather_df):
    airport_weather_df = mapping_df.join(weather_df, on="station_id", how="inner").select(
        "iata_code",
        "year",
        "month",
        "avg_temperature",
        "total_precipitation",
        "total_snowfall"
    )

    return airport_weather_df


def create_gold_dataset(
        flight_agg_df,
        throughput_agg_df,
        wait_time_agg_df,
        airport_weather_df
):
    gold_df = flight_agg_df \
        .join(
            throughput_agg_df,
            on=["iata_code", "year", "month"],
            how="left") \
        .join(
            wait_time_agg_df,
            on=["iata_code", "year", "month"],
            how="left") \
        .join(
            airport_weather_df,
            on=["iata_code", "year", "month"],
            how="left")
    
    return gold_df


def calculate_passenger_to_flight_ratio(df):
    df = df.withColumn(
        "passenger_to_flight_ratio",
        F.when(
            F.col("total_flights") > 0,
            F.col("total_passenger_count") / F.col("total_flights")
        )
    )

    return df


def round_numeric_columns(df, numeric_columns):
    for column in numeric_columns:
        df = df.withColumn(column, F.round(column, 2))
    
    return df
