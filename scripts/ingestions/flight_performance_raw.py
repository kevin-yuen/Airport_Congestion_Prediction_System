import scripts.utils.utils as u


def load_flight_performance_data(incoming_path, spark):
    _, csv_files = u.get_files_by_year(
        incoming_path,
        spark,
        2013,
        2015
    )

    raw_df = spark.read.option("header", True).option("inferSchema", True).csv(csv_files)

    return csv_files, raw_df