import scripts.ingestions.flight_performance_raw as flight_performance_ingestion
import scripts.transformations.flight_performance_cleaned as flight_performance_transformation
import scripts.utils.dq_transformation_utils as dq_transformation
import scripts.utils.dq_ingestion_utils as idq
import scripts.utils.utils as u
import constants as c
import pyspark.sql.functions as F


def run_flight_performance_incremental_job(
        incoming_path, 
        archived_path, 
        spark, 
        raw_parquet_path,
        transformed_parquet_path):
    # Ingestion
    fp_ingestion_dq = idq.IngestionDataQuality()
    csv_files, fp_raw_df = flight_performance_ingestion.load_flight_performance_data(
        incoming_path, 
        spark)

    # DQ ingestion: verify combined row counts of all csv files == raw dataset total row count
    print("\n--------------- DQ INGESTION ---------------\n")
    fp_ingestion_dq.verify_source_file_row_counts(csv_files)
    fp_ingestion_dq.log_raw_df_count(fp_raw_df)
    fp_ingestion_dq.verify_total_row_counts()

    # Identify partitions affected by the new data for incremental updates
    affected_partitions = u.get_affected_partitions(fp_raw_df, "YEAR")
    u.incremental_updates(
        spark,
        fp_raw_df,
        raw_parquet_path,
        "YEAR",
        affected_partitions)

    fp_raw_parquet_df = spark.read.parquet(raw_parquet_path)

    # DQ pre-transform
    print("\n--------------- DQ BEFORE TRANSFORMATION ---------------\n")
    # inspect schema (field type, nullability)
    fp_raw_parquet_df.printSchema()
    # get row count from the raw dataset
    print(fp_raw_parquet_df.count())
    # inspect nullable columns
    dq_transformation.check_null_counts(fp_raw_parquet_df)
    # inspect column uniqueness
    columns = ["YEAR", "MONTH", "DAY_OF_MONTH", "OP_CARRIER", "OP_CARRIER_FL_NUM", "ORIGIN", "DEST", "CRS_DEP_TIME"]
    dq_transformation.check_column_uniqueness(fp_raw_parquet_df, columns)

    # Transformation    
    columns = [raw_col for raw_col, _ in c.FLIGHT_PERFORMANCE_COLUMN_MAPPING.items()]
    flight_df = flight_performance_transformation.select_and_rename_columns(
        fp_raw_parquet_df, 
        columns, 
        c.FLIGHT_PERFORMANCE_COLUMN_MAPPING)
    
    ## type casting
    flight_df = (
        flight_df
        .withColumn(
            "is_cancelled", 
            F.col("is_cancelled").cast("boolean"))
        .withColumn(
            "is_diverted",
            F.col("is_diverted").cast("boolean"))
        .withColumn(
            "flight_count",
            F.col("flight_count").cast("integer"))
    )
    
    ## impute missing values
    ### impute missing scheduled flight duration (scheduled_elapsed_time)
    condition_scheduled = F.col("scheduled_elapsed_time").isNotNull()
    imputed_scheduled_time_df = flight_performance_transformation.conditional_impute(
        flight_df, 
        condition_scheduled, 
        "scheduled_elapsed_time", 
        "avg_scheduled_duration")

    ### impute missing actual flight duration (actual_elapsed_time) for non-cancelled flights
    condition_actual = (F.col("is_cancelled") == False) & (F.col("actual_elapsed_time").isNotNull())
    imputed_actual_time_df = flight_performance_transformation.conditional_impute(
        imputed_scheduled_time_df, 
        condition_actual, 
        "actual_elapsed_time",
        "avg_actual_duration")
    
    ### all non-cancelled flights have taxi out time

    ### impute missing taxi in time for non-cancelled flights    
    condition_taxi_in = (F.col("is_cancelled") == False) & (F.col("taxi_in_time").isNotNull())
    imputed_taxi_in_df = flight_performance_transformation.conditional_impute(
        imputed_actual_time_df,
        condition_taxi_in,
        "taxi_in_time",
        "avg_taxi_in_time"
    )

    ### impute missing values with 0 for:
    ### carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay
    delay_cols = ["carrier_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"]
    imputed_delays_df = imputed_taxi_in_df.fillna(0, subset=delay_cols)

    ## create primary key
    fp_cleaned = imputed_delays_df.withColumn(
        "flight_pk", 
        F.sha2(F.concat_ws(
            "||", 
            F.col("year"), 
            F.col("day_of_month"), 
            F.col("carrier_code"), 
            F.col("flight_number"), 
            F.col("origin_iata_code"), 
            F.col("destination_iata_code"),
            F.col("scheduled_elapsed_time")), 256)
    ).dropDuplicates(["flight_pk"])

    # reorder columns
    columns_reordered = ["flight_pk"] + flight_df.columns
    fp_cleaned = fp_cleaned.select(*columns_reordered)
    
    # DQ post-transform
    print("\n--------------- DQ AFTER TRANSFORMATION ---------------\n")
    fp_cleaned.printSchema()
    
    print(f"Row count: {str(fp_cleaned.count())}")

    imputed_delays_df.filter(
        (F.col('carrier_delay').isNull()) | 
        (F.col('weather_delay').isNull()) | 
        (F.col('nas_delay').isNull()) | 
        (F.col('security_delay').isNull()) | 
        (F.col('late_aircraft_delay').isNull())
    ).show()

    # incremental update transformed df
    u.incremental_updates(
        spark,
        fp_cleaned,
        transformed_parquet_path,
        "year",
        affected_partitions
    )

    # archive partitioned source files
    for partition in affected_partitions:
        u.archive_partition(
            incoming_path,
            archived_path,
            f"year={partition}"
        )
