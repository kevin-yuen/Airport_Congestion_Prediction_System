import constants as c
import pyspark.sql.functions as F

import scripts.utils.utils as u
import scripts.ingestions.dept_performance_raw as dp_ingestion
import scripts.utils.dq_ingestion_utils as dq_ingestion
import scripts.transformations.ontime_dept_performance_cleaned as dp_transformation
import scripts.utils.dq_transformation_utils as dq_transformation


def run_departure_performance_job(
        incoming_path,
        archived_path,
        transformed_csv_path,
        spark
    ):
    # Ingestion
    csv_files, departure_performance_raw_df = dp_ingestion.load_departure_performance_data(
        incoming_path,
        spark)


    # DQ ingestion: verify combined row counts of all csv files == raw dataset total row count
    dq_ingestion.log_ingestion_summary(csv_files, departure_performance_raw_df)


    # DQ pre-transform
    print("\n--------------- DQ BEFORE TRANSFORMATION ---------------\n")
    # inspect schema (field type, nullability)
    departure_performance_raw_df.printSchema()
    # inspect nullable columns
    dq_transformation.check_null_counts(departure_performance_raw_df)
    # get row with null rank, airport_location, performance (%)
    departure_performance_raw_df.filter(F.isnull(F.col('rank'))).show()
    # get max and min performance value
    dq_transformation.check_max_and_min(departure_performance_raw_df, 'performance (%)')


    # Transformation
    dp_non_nulls_df = departure_performance_raw_df.filter(F.isnotnull(F.col('rank')))
    dp_cleaned_loc = dp_transformation.clean_airport_location(dp_non_nulls_df)
    dp_mapped_states = dp_transformation.map_state_name(dp_cleaned_loc)

    cols = ['iata_code', 'year', 'state_code', 'state_name', 'city_name', 'ontime_departure_performance']
    dp_renamed_cols_df = dp_mapped_states.withColumnsRenamed(c.DEPT_COLUMN_MAPPING).select(*cols)

    ontime_dept_performance_cleaned = dp_renamed_cols_df.filter((F.col('year') >= 2013) & (F.col('year') <= 2015))

    ontime_dept_performance_cleaned = ontime_dept_performance_cleaned.withColumn(
        "year",
        F.col("year").cast("integer")
    )


    # DQ post-transform
    print("\n--------------- DQ AFTER TRANSFORMATION ---------------\n")
    row_count_pretransform = departure_performance_raw_df.count()
    row_count_posttransform = dp_renamed_cols_df.count()        # before filter years 2013 - 2015
    print(f"Row count before transformation: {row_count_pretransform}")                 # 665
    print(f"Row count after transformation (all years): {row_count_posttransform}")     # 664 after 'null' rank filtered out

    ontime_dept_performance_cleaned.filter(
        (F.isnull(F.col('city_name'))) | 
        (F.isnull(F.col('state_code'))) | 
        (F.isnull(F.col('iata_code'))) | 
        (F.isnull(F.col('state_name'))) |
        (F.isnull(F.col('rank')))).show()
    
    ontime_dept_performance_cleaned.select('year').distinct().show()

    # final schema check
    ontime_dept_performance_cleaned.printSchema()

    # is (iata_code + year) unique?
    print(f"Row count after transformation (2013-2015): {ontime_dept_performance_cleaned.count()}")     # 87
    ontime_dept_performance_cleaned.select(F.count_distinct('iata_code', 'year')).show()    # 87

    ontime_dept_performance_cleaned.write.format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save(transformed_csv_path)

    ontime_dept_performance_cleaned.show(10)

    for csv_file in csv_files:
        u.move_file_to_archived(csv_file, archived_path)
