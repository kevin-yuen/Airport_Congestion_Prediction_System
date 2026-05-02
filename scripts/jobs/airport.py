import scripts.ingestions.airport_raw as airport_ingestion
import scripts.utils.dq_ingestion_utils as idq


def run_airport_job(root_path, spark):
    airport_idq = idq.IngestionDataQuality()

    csv_files, airport_raw_df = airport_ingestion.load_airport_data(root_path, spark)

    # DQ ingestion: verify combined row counts of all csv files == raw dataset total row count
    airport_idq.verify_source_file_row_counts(csv_files)
    airport_idq.log_raw_df_count(airport_raw_df)
    airport_idq.verify_total_row_counts()
