import scripts.ingestions.flight_performance_raw as flight_performance_ingestion
import scripts.utils.dq_ingestion_utils as idq


def run_flight_performance_job(root_path, spark):
    flight_performance_idq = idq.IngestionDataQuality()

    csv_files, flight_performance_raw_df = flight_performance_ingestion.load_flight_performance_data(root_path, spark)

    # DQ ingestion: verify combined row counts of all csv files == raw dataset total row count
    flight_performance_idq.verify_source_file_row_counts(csv_files)
    flight_performance_idq.log_raw_df_count(flight_performance_raw_df)
    flight_performance_idq.verify_total_row_counts()