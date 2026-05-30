import pyspark.sql.functions as F
from pyspark.sql import Window


def calculate_distance(df):
    # calculate distance between airport and corresponding weather stations in miles
    df = df.withColumn(
        "distance_miles",
        (
            3958.8 * 2 * F.asin(
                F.sqrt(
                    F.pow(
                        F.sin(
                            (
                                F.radians("station_lat") -
                                F.radians("airport_lat")
                            ) / 2
                        ),
                        2
                    ) +
                    F.cos(F.radians("airport_lat")) *
                    F.cos(F.radians("station_lat")) *
                    F.pow(
                        F.sin(
                            (
                                F.radians("station_lon") -
                                F.radians("airport_lon")
                            ) / 2
                        ),
                        2
                    )
                )
            )
        )
    )

    return df


def get_nearest_station(df):
    # only keep the nearest weather station
    w = (
        Window
        .partitionBy("iata_code")
        .orderBy("distance_miles")
    )

    mapping_df = (
        df
        .withColumn(
            "distance_rank",
            F.row_number().over(w)
        )
        .filter(
            F.col("distance_rank") == 1
        )
        .select(
            "iata_code",
            "station_id",
            F.round(
                "distance_miles",
                2
            ).alias("distance_miles")
        )
        .dropDuplicates([
            "iata_code",
            "station_id"
        ])
    )
    
    return mapping_df