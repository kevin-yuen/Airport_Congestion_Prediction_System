from pyspark.sql.types import StructType, StructField, StringType
import os


def load_weather_data(incoming_path, spark):
    schema = StructType([
        StructField("CLDD", StringType(), True),
        StructField("DATE", StringType(), True),
        StructField("ELEVATION", StringType(), True),
        StructField("HTDD", StringType(), True),
        StructField("LATITUDE", StringType(), True),
        StructField("LONGITUDE", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("PRCP", StringType(), True),
        StructField("SNOW", StringType(), True),
        StructField("STATION", StringType(), True),
        StructField("TAVG", StringType(), True),
        StructField("TMAX", StringType(), True),
        StructField("TMIN", StringType(), True),
    ])

    raw_df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("basePath", incoming_path)
        .schema(schema)
        .csv(
            os.path.join(
                incoming_path,
                "state=*/*.csv"
            )
        )
        .cache()
    )

    print(f"[INFO] Weather raw dataset row count: {raw_df.count()}")

    return raw_df