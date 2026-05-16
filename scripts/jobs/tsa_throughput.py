import pyspark.sql.functions as F
from itertools import chain

import scripts.ingestions.tsa_throughput_raw as tp_ingestion
import scripts.transformations.tsa_throughput_cleaned as tp_transformation
import scripts.utils.dq_transformation_utils as dq_transformation
import scripts.utils.utils as u
import constants as c


def run_tsa_throughput_incremental_job(
    incoming_path,
    archived_path,
    spark,
    raw_parquet_path,
    transformed_parquet_path
):
    # ---------------------------------------------------
    # [START] INGESTION
    # ---------------------------------------------------
    # ingest raw data for TSA throughput
    tp_raw_df = tp_ingestion.load_tp_data(incoming_path, spark)
    
    # immediate transformations to forward-fill nulls and remove 'Date' rows
    tp_raw_df = tp_raw_df.dropna(thresh=2)     # remove blank rows for year 2014 and 2015
    tp_raw_df = tp_transformation.remove_header_marker_rows(tp_raw_df)
    tp_raw_df = tp_transformation.fill_nulls(tp_raw_df)
    temp_cleaned_df = tp_raw_df.dropDuplicates()    # ensure no duplicate throughput for the same airport checkpoint and date/time

    print(f"[INFO] TSA Throughput raw dataset row count: {temp_cleaned_df.count()}")

    # write incremental updates to the affected raw parquet dataset
    affected_partitions = u.get_affected_partitions(temp_cleaned_df, "year")
    u.incremental_updates(
        spark,
        temp_cleaned_df,
        raw_parquet_path,
        "year",
        affected_partitions
    )

    # archive processed partitions
    for year in affected_partitions:
        u.archive_partition(incoming_path, archived_path, f"year={year}")
    # ---------------------------------------------------
    # [END] INGESTION
    # ---------------------------------------------------


    # read raw parquet dataset for downstream transformations
    tp_raw_parquet_df = spark.read.parquet(raw_parquet_path).cache()

    # ---------------------------------------------------
    # [START] DQ CHECK BEFORE TRANSFORMATION
    # ---------------------------------------------------
    print("""\n***********************************************
*********** DQ BEFORE TRANSFORMATION ***********
************************************************\n""")
    # row count check (raw df vs raw parquet df)
    dq_transformation.check_row_counts(
        temp_cleaned_df,
        tp_raw_parquet_df,
        "year",
        affected_partitions,
        "raw"
    )
    # schema check
    tp_raw_parquet_df.printSchema()
    # null check
    dq_transformation.check_null_counts(tp_raw_parquet_df)
    # uniqueness check
    dq_transformation.check_column_uniqueness(
        tp_raw_parquet_df,
        tp_raw_parquet_df.columns
    )
    # sample data inspection
    for year in affected_partitions:
        tp_raw_parquet_df.filter(tp_raw_parquet_df["year"] == year).show(10)

    # get max and min throughput
    type_casted_df = tp_raw_parquet_df.withColumn(
        "passenger_count",
        F.col("passenger_count").cast("integer")
    )

    type_casted_df.printSchema()        # confirm schema after type casting
    # inspect sample data
    type_casted_df.show(10)
    dq_transformation.check_max_and_min(type_casted_df, "passenger_count")
    # ---------------------------------------------------
    # [END] DQ CHECK BEFORE TRANSFORMATION
    # ---------------------------------------------------


    # ---------------------------------------------------
    # [START] TRANSFORMATION
    # ---------------------------------------------------
    # get state names and remove "invalid" state codes
    mapping_expr = F.create_map([F.lit(s) for s in chain(*c.STATES.items())])
    mapped_df = type_casted_df.withColumn(
        "state_name", 
        mapping_expr[F.col("state_code")]).filter(F.col("state_name").isNotNull())

    # fix bad dates (e.g. 12/16/201)
    standardized_df = mapped_df.withColumn(
        "date",
        F.when(
            F.col("date").rlike(r"^\d{1,2}/\d{1,2}/\d{3}$"),
            F.concat(
                F.col("date"),
                F.substring(F.col("year"), 4, 1)
            )
        ).otherwise(F.col("date"))
    )

    # standardize date values
    standardized_df = standardized_df.withColumn(
        "date",
        F.date_format(
            F.coalesce(
                F.to_timestamp(F.trim(F.col("date")), "M/d/yyyy H:mm"),
                F.to_timestamp(F.trim(F.col("date")), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.trim(F.col("date")), "M/d/yyyy"),
                F.to_timestamp(F.trim(F.col("date")), "MM/dd/yyyy"),
                F.to_timestamp(F.trim(F.col("date")), "MM/dd/yyyy HH:mm"),
                F.to_timestamp(F.trim(F.col("date")), "yyyy-MM-dd")
            ),
            "yyyy-MM-dd"
        )
    )

    # remove date = '4"' and date = '5"'
    standardized_df = standardized_df.dropna(subset=["date"])

    # some or all records in the 2013 throughput partition contain 2014 data; 
    # update the 'year' column accordingly
    standardized_df = standardized_df.withColumn(
        "year",
        F.when(
            (F.substring("date", 1, 4) == "2014") & (F.col("year") == 2013),
            2014
        ).otherwise(F.col("year"))
    )

    # sum up passenger count
    aggregated_df = tp_transformation.aggregate_tp(standardized_df)

    # reorder columns and drop duplicates
    new_order = [
        "iata_code", "checkpoint", "date", "airport", "city", 
        "state_code", "state_name", "year", "passenger_count"
    ]
    tp_cleaned_df = aggregated_df.select(*new_order).dropDuplicates()
    # ---------------------------------------------------
    # [END] TRANSFORMATION
    # ---------------------------------------------------


    # ---------------------------------------------------
    # [START] DQ CHECK AFTER TRANSFORMATION
    # ---------------------------------------------------
    print("""\n***********************************************
*********** DQ AFTER TRANSFORMATION ***********
************************************************\n""")
    # final schema check
    tp_cleaned_df.printSchema()
    # null check
    dq_transformation.check_null_counts(tp_cleaned_df)
    # distinct row count check
    cols =  [
        "iata_code",
        "checkpoint",
        "date",
        "airport",
        "city",
        "state_code",
        "state_name",
        "year"  
    ]
    dq_transformation.check_distinct_row_count(tp_cleaned_df, cols)
    # sample data check
    tp_cleaned_df.filter(tp_cleaned_df["year"] == 2013).show(50)
    tp_cleaned_df.filter(tp_cleaned_df["year"] == 2014).show(50)
    tp_cleaned_df.filter(tp_cleaned_df["year"] == 2015).show(50)
    # ---------------------------------------------------
    # [END] DQ CHECK AFTER TRANSFORMATION
    # ---------------------------------------------------


    # write incremental updates to the affected transformed parquet dataset
    u.incremental_updates(
        spark,
        tp_cleaned_df,
        transformed_parquet_path,
        "year",
        affected_partitions
    )

    tp_raw_parquet_df.unpersist()
