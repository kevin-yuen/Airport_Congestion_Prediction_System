import scripts.ingestions.dept_performance_raw as dept_performance_ingestion
import scripts.utils.dq_ingestion_utils as idq
import scripts.utils.dq_pretransform_utils as dq_pretransform
import scripts.transformations.ontime_dept_performance_cleaned as dept_performance_transformation
import pyspark.sql.functions as F
import constants as c


def run_departure_performance_job(root_path, spark):
    departure_performance_idq = idq.IngestionDataQuality()

    csv_files, departure_performance_raw_df = dept_performance_ingestion.load_departure_performance_data(root_path, spark)

    # DQ ingestion: verify combined row counts of all csv files == raw dataset total row count
    departure_performance_idq.verify_source_file_row_counts(csv_files)
    departure_performance_idq.log_raw_df_count(departure_performance_raw_df)
    departure_performance_idq.verify_total_row_counts()

    # DQ pre-transform departure performance:
    # inspect schema (field type, nullability)
    departure_performance_raw_df.printSchema()
    # inspect nullable columns
    dq_pretransform.count_nulls(departure_performance_raw_df)
    # get row with null rank, airport_location, performance (%)
    departure_performance_raw_df.filter(F.isnull(F.col('rank'))).show()
    # get max and min performance value
    dq_pretransform.get_max_and_min(departure_performance_raw_df, 'performance (%)')

    # Transformation: departure performance
    dept_performance_non_nulls_df = dept_performance_transformation.remove_nulls(departure_performance_raw_df, 'rank')
    dept_performance_cleaned_loc = dept_performance_transformation.clean_airport_location(dept_performance_non_nulls_df)
    dept_performance_mapped_states = dept_performance_transformation.map_state_name(dept_performance_cleaned_loc)
    dept_performance_renamed_columns = dept_performance_transformation.rename_columns(dept_performance_mapped_states, c.DEPT_COLUMN_MAPPING)
    
    cols = ['iata_code', 'year', 'state_code', 'state_name', 'city_name', 'ontime_departure_performance']
    ontime_dept_performance_cleaned = dept_performance_transformation.select_columns(dept_performance_renamed_columns, cols)

    ontime_dept_performance_cleaned.show(5)

    # DQ post-transform departure performance:
    ontime_dept_performance_cleaned.filter(
        (F.isnull(F.col('city_name'))) | 
        (F.isnull(F.col('state_code'))) | 
        (F.isnull(F.col('iata_code'))) | 
        (F.isnull(F.col('state_name'))) |
        (F.isnull(F.col('rank')))).show()
    
    row_count_pretransform = departure_performance_raw_df.count()
    row_count_posttransform = ontime_dept_performance_cleaned.count()

    print(f"Row count before transformation: {row_count_pretransform}")
    print(f"Row count after transformation: {row_count_posttransform}")
