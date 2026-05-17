import pyspark.sql.functions as F
from itertools import chain
import os
import glob
import pandas as pd

import scripts.ingestions.weather_raw as w_ingestion
import scripts.transformations.tsa_throughput_cleaned as tp_transformation
import scripts.utils.dq_transformation_utils as dq_transformation
import scripts.utils.utils as u
import constants as c


def _preprocess_raw_files(
        raw_path, 
        raw_archived_path, 
        output_path, 
    ):
    state_folders = glob.glob(os.path.join(raw_path, "state=*"))

    if not state_folders:
        raise FileNotFoundError("No partition folder found.")

    for state_folder in state_folders:
        partition_name = os.path.basename(state_folder)

        print(f"\n[PROCESSING PARTITION] {partition_name}")

        # create matching partition folder in incoming/
        output_partition_folder = os.path.join(output_path, partition_name)
        os.makedirs(output_partition_folder, exist_ok=True)

        csv_files = glob.glob(os.path.join(state_folder, "*.csv"))

        for csv_file in csv_files:
            try:
                print(f"Processing: {csv_file}")

                df = pd.read_csv(csv_file)
                existing_columns = [col for col in c.WEATHER_REQUIRED_COLUMNS if col in df.columns]

                if len(existing_columns) < len(c.WEATHER_REQUIRED_COLUMNS):
                    print(
                        "[WARNING] Source file is missing one or more required columns "
                        "and will be excluded from downstream processing."
                    )
                    continue

                cleaned_df = df[existing_columns]
                cleaned_df = cleaned_df.sort_index(axis=1)

                # preserve original filename
                output_file_path = os.path.join(output_partition_folder, os.path.basename(csv_file))

                # write cleaned csv to incoming/
                cleaned_df.to_csv(output_file_path,index=False)

                print(f"[SUCCESS] Saved cleaned file -> {output_file_path}")
            except Exception as e:
                print(f"[ERROR] {csv_file}\n{e}")

        # after preprocessing the current partition's raw files, move the partition to raw_archived
        state_code = state_folder.split("=")[-1]
        u.archive_partition(raw_path, raw_archived_path, f"state={state_code}")


def run_tsa_weather_incremental_job(
    base_path,
    incoming_path,
    incoming_archived_path,
    spark,
    raw_parquet_path,
    transformed_parquet_path
):
    # column headers vary across source files and partitions
    # preprocess the raw source files to standardize the required
    # headers for consistent downstream processing
    raw_path = os.path.join(base_path, "raw")
    raw_archived_path = os.path.join(base_path, "raw_archived")
    _preprocess_raw_files(raw_path, raw_archived_path, incoming_path)


    # ---------------------------------------------------
    # [START] INGESTION
    # ---------------------------------------------------
    # ingest raw data for weather
    weather_raw_df = w_ingestion.load_weather_data(incoming_path, spark)

    # write incremental updates to the affected raw parquet dataset
    affected_partitions = u.get_affected_partitions(weather_raw_df, "state")
    u.incremental_updates(
        spark,
        weather_raw_df,
        raw_parquet_path,
        "state",
        affected_partitions
    )
  
    # archive processed partitions
    for state in affected_partitions:
        u.archive_partition(incoming_path, incoming_archived_path, f"state={state}")
    # ---------------------------------------------------
    # [END] INGESTION
    # ---------------------------------------------------

    # read raw parquet dataset for downstream transformations
    w_raw_parquet_df = spark.read.parquet(raw_parquet_path).cache()


    # ---------------------------------------------------
    # [START] DQ CHECK BEFORE TRANSFORMATION
    # ---------------------------------------------------
    print("""\n***********************************************
*********** DQ BEFORE TRANSFORMATION ***********
************************************************\n""")
    # row count check (raw df vs raw parquet df)
    dq_transformation.check_row_counts(
        weather_raw_df,
        w_raw_parquet_df,
        "state",
        affected_partitions,
        "raw"
    )
    # schema check
    w_raw_parquet_df.printSchema()
    # null check
    dq_transformation.check_null_counts(w_raw_parquet_df)
    # uniqueness check
    dq_transformation.check_column_uniqueness(
        w_raw_parquet_df,
        w_raw_parquet_df.columns
    )
    # sample data inspection
    for state in affected_partitions:
        w_raw_parquet_df.filter(w_raw_parquet_df["state"] == state).show(10)

    # get max and min weather data
    type_casted_df = w_raw_parquet_df.withColumns({
        "CLDD": F.col("CLDD").cast("double"),
        "ELEVATION": F.col("ELEVATION").cast("double"),
        "HTDD": F.col("HTDD").cast("double"),
        "LATITUDE": F.col("LATITUDE").cast("double"),
        "LONGITUDE": F.col("LONGITUDE").cast("double"),
        "PRCP": F.col("PRCP").cast("double"),
        "SNOW": F.col("SNOW").cast("double"),
        "TAVG": F.col("TAVG").cast("double"),
        "TMAX": F.col("TMAX").cast("double"),
        "TMIN": F.col("TMIN").cast("double"),
    })

    type_casted_df.printSchema()        # confirm schema after type casting
    # inspect sample data
    type_casted_df.show(10)

    cols = ["CLDD", "ELEVATION", "HTDD", "LATITUDE",
            "LONGITUDE", "PRCP", "SNOW", "TAVG", "TMAX", "TMIN"]
    for col in cols:
        print(f"\n****** {col} MAX and MIN ******")
        dq_transformation.check_max_and_min(type_casted_df, col)

    # inspect elevation for possible transformation
    type_casted_df.filter(F.col("ELEVATION") < 0).show(20)  # acceptable; NEW ORLEANS is below or near sea level

    # inspect high outliers for SNOW
    type_casted_df.orderBy(F.desc("SNOW")).show(20)     # acceptable; STAMPEDE PASS, WA US is heavy-snow

    # more DQ rules...
    print(f"Any rows with latitude greater than 90? {type_casted_df.filter(F.col('LATITUDE') > 90).count()}")
    print(f"Any rows with longitude greater than 90? {type_casted_df.filter(F.col('LONGITUDE') > 180).count()}")
    print(f"Any rows with TMIN greater than TMAX? {type_casted_df.filter(F.col('TMAX') < F.col('TMIN')).count()}")
    print(f"Any rows with elevation lower than -100? {type_casted_df.filter(F.col('ELEVATION') < -100).count()}")
    # ---------------------------------------------------
    # [END] DQ CHECK BEFORE TRANSFORMATION
    # ---------------------------------------------------


    # ---------------------------------------------------
    # [START] TRANSFORMATION
    # ---------------------------------------------------
    temp_cleaned_df = type_casted_df.withColumnsRenamed({
        "DATE": "date",
        "STATION": "station_id",
        "NAME": "station_name",
        "state": "state_code",
        "LATITUDE": "latitude",
        "LONGITUDE": "longitude",
        "TAVG": "avg_temperature",
        "TMAX": "max_temperature",
        "TMIN": "min_temperature",
        "PRCP": "total_precipitation",
        "SNOW": "total_snowfall",
        "CLDD": "cooling_degree_days",
        "HTDD": "heating_degree_days"
    })

    mapping_expr = F.create_map([F.lit(s) for s in chain(*c.STATES.items())])
    temp_cleaned_df = temp_cleaned_df.withColumn(
        "state_name", 
        mapping_expr[F.col("state_code")]).filter(F.col("state_name").isNotNull())

    temp_cleaned_df = temp_cleaned_df.withColumns({
        "country_code": F.element_at(F.split(F.col("station_name"), " "), -1),
        "station_name": F.element_at(F.split(F.col("station_name"), ","), 1),
        "year": F.element_at(F.split(F.col("date"), "-"), 1),
        "month": F.element_at(F.split(F.col("date"), "-"), 2),
    })

    cols = [
        "station_id", "year", "month", "latitude", "longitude",
        "station_name", "state_code", "state_name", "country_code",
        "avg_temperature", "max_temperature", "min_temperature",
        "total_precipitation", "total_snowfall", "cooling_degree_days",
        "heating_degree_days"
    ]

    df_cleaned = temp_cleaned_df.withColumns({
        "year": F.col("year").cast("integer"),
        "month": F.col("month").cast("integer")
    }).select(*cols)

    print(f"Total rows before de-duplication: {df_cleaned.count()}")
    df_cleaned = df_cleaned.dropDuplicates()
    # ---------------------------------------------------
    # [END] TRANSFORMATION
    # ---------------------------------------------------


     # ---------------------------------------------------
    # [START] DQ CHECK AFTER TRANSFORMATION
    # ---------------------------------------------------
    print("""\n***********************************************
*********** DQ AFTER TRANSFORMATION ***********
************************************************\n""")
    # final row count check
    print(f"Total rows after de-duplication: {df_cleaned.count()}")
    # final schema check
    df_cleaned.printSchema()
    # null check
    dq_transformation.check_null_counts(df_cleaned)
    # sample data check
    df_cleaned.show(50, truncate=False)
    # ---------------------------------------------------
    # [END] DQ CHECK AFTER TRANSFORMATION
    # ---------------------------------------------------


    # write incremental updates to the affected transformed parquet dataset
    u.incremental_updates(
        spark,
        df_cleaned,
        transformed_parquet_path,
        "state_code",
        affected_partitions
    )

    w_raw_parquet_df.unpersist()
