import pyspark.sql.functions as F
from pyspark.sql import Window    


def fill_nulls(raw_df):
    raw_df = raw_df.withColumn("row_num", F.monotonically_increasing_id())
    w = Window.partitionBy("year").orderBy("row_num").rowsBetween(Window.unboundedPreceding, 0)

    filled_df = raw_df.withColumns({
        "date": F.last("date", ignorenulls=True).over(w),
        "hour": F.last("hour", ignorenulls=True).over(w),
        "iata_code": F.last("iata_code", ignorenulls=True).over(w),
        "airport": F.last("airport", ignorenulls=True).over(w),
        "city": F.last("city", ignorenulls=True).over(w),
        "state_code": F.last("state_code", ignorenulls=True).over(w),
        "checkpoint": F.last("checkpoint", ignorenulls=True).over(w),
        # "passenger_count": F.last("checkpoint", ignorenulls=True).over(w)
    }).drop("row_num")
    return filled_df


def remove_header_marker_rows(transformed_df):
    date_rows = transformed_df.filter(transformed_df["date"] == "Date").count()
    print(f"[INFO] Number of rows with 'Date': {date_rows}")

    transformed_df = transformed_df.filter(
        (transformed_df["date"] != "Date") |
        (F.col("date").isNull()))
    print(f"[INFO] Row count after removing 'Date' rows: {transformed_df.count()}")

    return transformed_df


def aggregate_tp(df):
    aggregated_df = (
        df.filter(F.col("passenger_count").isNotNull())
        .groupBy(
            "iata_code",
            "checkpoint",
            "date",
            "airport",
            "city",
            "state_code",
            "state_name",
            "year"
        )
        .agg(
            F.sum(
                F.col("passenger_count")
            ).alias("passenger_count")
        )

    )

    return aggregated_df