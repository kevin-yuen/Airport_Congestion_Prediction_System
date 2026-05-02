import scripts.ingestions.dept_performance_raw as dept_performance_ingestion
import scripts.utils.dq_ingestion_utils as idq


def run_departure_performance_job(root_path, spark):
    departure_performance_idq = idq.IngestionDataQuality()

    csv_files, departure_performance_raw_df = dept_performance_ingestion.load_departure_performance_data(root_path, spark)

    # DQ ingestion: verify combined row counts of all csv files == raw dataset total row count
    departure_performance_idq.verify_source_file_row_counts(csv_files)
    departure_performance_idq.log_raw_df_count(departure_performance_raw_df)
    departure_performance_idq.verify_total_row_counts()