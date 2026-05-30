import constants as c
import pyspark.sql.functions as F
import scripts.utils.utils as u
import scripts.ingestions.airport_raw as a_ingestion
import scripts.utils.dq_ingestion_utils as dq_ingestion
import scripts.transformations.airport_cleaned as a_transformation
import scripts.utils.dq_transformation_utils as dq_transformation


def run_airport_job(
        incoming_path,
        archived_path,
        transformed_csv_path,
        spark
    ):
    # Ingestion
    airport_files, airport_raw_df = a_ingestion.load_airport_data(incoming_path, spark)


    # DQ ingestion: verify combined row counts of all csv files == raw dataset total row count
    dq_ingestion.log_ingestion_summary(airport_files, airport_raw_df)


    # DQ pre-transform
    print("\n--------------- DQ BEFORE TRANSFORMATION ---------------\n")
    # inspect schema (field type, nullability)
    airport_raw_df.printSchema()        
    # validate column uniqueness
    dq_transformation.check_column_uniqueness(airport_raw_df, airport_raw_df.columns)
    # count nulls
    dq_transformation.check_null_counts(airport_raw_df)


    # Transformation
    columns = [raw_col for raw_col, _ in c.AIRPORT_COLUMN_MAPPING.items()]
    # airport_selected_and_renamed_df = a_transformation.select_and_rename_columns(airport_raw_df, columns, c.AIRPORT_COLUMN_MAPPING)
    airport_df = airport_raw_df.select(*columns).withColumnsRenamed(c.AIRPORT_COLUMN_MAPPING)


    # airport_active_df = a_transformation.get_active_airport(airport_selected_and_renamed_df)
    airport_active_df = airport_df.filter((airport_df['is_closed'] == 0) & (airport_df['is_latest'] == 1))

    airport_clean_city_df = a_transformation.clean_city(airport_active_df)
    airport_active_BER_df = a_transformation.get_active_ber_airport(airport_clean_city_df)

    clean_date_columns = ['start_date', 'close_date']
    airport_clean_dates_df = a_transformation.clean_dates(airport_active_BER_df, clean_date_columns)

    # airport_selected_year_df = a_transformation.get_data_by_year(airport_clean_dates_df, 2015)
    airport_selected_year_df = airport_clean_dates_df.filter(F.col('start_date_year') <= 2015)

    # clean up the dataset by dropping unused columns and renaming columns
    drop_columns = [
        "start_date", 
        "start_date_ts", 
        "start_date_date", 
        "start_date_year",
        "close_date", 
        "close_date_ts",
        "close_date_date",
        "close_date_year"
    ]

    col_mapping = {
        "start_date_year_month": "start_year_month",
        "close_date_year_month": "close_year_month",
    }
    # airport_cleaned = a_transformation.drop_and_rename_columns(airport_selected_year_df, drop_columns, col_mapping)
    airport_cleaned = airport_selected_year_df.drop(*drop_columns).withColumnsRenamed(col_mapping)


    # DQ post-transform:
    print("\n--------------- DQ AFTER TRANSFORMATION ---------------\n")
    # any airport closed before year 2013?
    airport_cleaned.filter(F.isnotnull(F.col('close_year_month'))).show()
    # is iata_code unique now?
    dq_transformation.check_column_uniqueness(airport_cleaned, ['iata_code'])
    # count nulls
    dq_transformation.check_null_counts(airport_cleaned)
    # final schema check
    airport_cleaned.printSchema()

    airport_cleaned.write.format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save(transformed_csv_path)

    # archive source files
    u.move_file_to_archived(airport_files[0], archived_path)
