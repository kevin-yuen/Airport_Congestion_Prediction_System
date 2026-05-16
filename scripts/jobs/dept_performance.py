import constants as c
import pyspark.sql.functions as F
import scripts.utils.utils as u
import scripts.ingestions.dept_performance_raw as dept_performance_ingestion
import scripts.utils.dq_ingestion_utils as idq
import scripts.transformations.ontime_dept_performance_cleaned as dept_performance_transformation
import scripts.utils.dq_transformation_utils as dq_transformation


def run_departure_performance_job(incoming_path, archived_path, spark):
    # Ingestion
    departure_performance_idq = idq.IngestionDataQuality()
    csv_files, departure_performance_raw_df = dept_performance_ingestion.load_departure_performance_data(
        incoming_path,
        spark)


    # DQ ingestion: verify combined row counts of all csv files == raw dataset total row count
    print("\n--------------- DQ INGESTION ---------------\n")
    departure_performance_idq.verify_source_file_row_counts(csv_files)
    departure_performance_idq.log_raw_df_count(departure_performance_raw_df)
    departure_performance_idq.verify_total_row_counts()


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
    dept_performance_non_nulls_df = dept_performance_transformation.remove_nulls(departure_performance_raw_df, 'rank')
    dept_performance_cleaned_loc = dept_performance_transformation.clean_airport_location(dept_performance_non_nulls_df)
    dept_performance_mapped_states = dept_performance_transformation.map_state_name(dept_performance_cleaned_loc)

    cols = ['iata_code', 'year', 'state_code', 'state_name', 'city_name', 'ontime_departure_performance']
    dept_performance_renamed_and_selected_cols = dept_performance_transformation.rename_and_select_columns(
        dept_performance_mapped_states,
        c.DEPT_COLUMN_MAPPING,
        cols
    )

    ontime_dept_performance_cleaned = dept_performance_transformation.get_data_by_year(
        dept_performance_renamed_and_selected_cols,
        'year',
        2013,
        2015
    )


    # DQ post-transform
    print("\n--------------- DQ AFTER TRANSFORMATION ---------------\n")
    row_count_pretransform = departure_performance_raw_df.count()
    row_count_posttransform = dept_performance_renamed_and_selected_cols.count()        # before filter years 2013 - 2015
    print(f"Row count before transformation: {row_count_pretransform}")                 # 665
    print(f"Row count after transformation (all years): {row_count_posttransform}")     # 664 after 'null' rank filtered out

    ontime_dept_performance_cleaned.filter(
        (F.isnull(F.col('city_name'))) | 
        (F.isnull(F.col('state_code'))) | 
        (F.isnull(F.col('iata_code'))) | 
        (F.isnull(F.col('state_name'))) |
        (F.isnull(F.col('rank')))).show()
    
    ontime_dept_performance_cleaned.select('year').distinct().show()

    # is (iata_code + year) unique?
    print(f"Row count after transformation (2013-2015): {ontime_dept_performance_cleaned.count()}")     # 87
    ontime_dept_performance_cleaned.select(F.count_distinct('iata_code', 'year')).show()    # 87

    ontime_dept_performance_cleaned.show(10)

    for csv_file in csv_files:
        u.move_file_to_archived(csv_file, archived_path)

    
