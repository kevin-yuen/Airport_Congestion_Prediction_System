import constants as c
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import Window as w
import scripts.ingestions.airport_raw as airport_ingestion
import scripts.utils.dq_ingestion_utils as idq
import scripts.utils.dq_pretransform_utils as dq_pretransform
import scripts.utils.dq_posttransform_utils as dq_posttransform
import scripts.transformations.airport_cleaned as airport_transformation


def run_airport_job(root_path, spark):
    airport_idq = idq.IngestionDataQuality()

    csv_files, airport_raw_df = airport_ingestion.load_airport_data(root_path, spark)

    # DQ ingestion: verify combined row counts of all csv files == raw dataset total row count
    airport_idq.verify_source_file_row_counts(csv_files)
    airport_idq.log_raw_df_count(airport_raw_df)
    airport_idq.verify_total_row_counts()

    # DQ pre-transform airport:
    # inspect schema (field type, nullability)
    airport_raw_df.printSchema()        
    # validate column uniqueness
    dq_pretransform.is_column_unique(airport_raw_df)
    # count nulls
    dq_pretransform.count_nulls(airport_raw_df)

    # Transformation: airport
    airport_selected_df = airport_transformation.select_columns(airport_raw_df)
    airport_renamed_df = airport_transformation.rename_columns(airport_selected_df, c.AIRPORT_COLUMN_MAPPING)
    airport_active_df = airport_transformation.get_active_airport(airport_renamed_df)
    airport_clean_city_df = airport_transformation.clean_city(airport_active_df)
    airport_cleaned = airport_transformation.get_active_BER(airport_clean_city_df)

    # DQ post-transform airport:
    # is iata_code unique?
    print("\nIs iata_code unique now?")
    dq_posttransform.check_column_uniqueness(airport_cleaned, 'iata_code')

    airport_cleaned.show(10)
