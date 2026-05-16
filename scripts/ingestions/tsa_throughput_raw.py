import os
from pyspark.sql.types import StructType, StructField, StringType


def load_tp_data(incoming_path, spark):
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("hour", StringType(), True),
        StructField("iata_code", StringType(), True),
        StructField("airport", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_code", StringType(), True),
        StructField("checkpoint", StringType(), True),
        StructField("metrics", StringType(), True),
        StructField("passenger_count", StringType(), True)
    ])


    raw_df = (
        spark.read
        .schema(schema)
        .option("header", False)
        .option("basePath", incoming_path)
        .csv(
            os.path.join(incoming_path, "year=*/*.csv")
        )
        .cache()
    )

    return raw_df