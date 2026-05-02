import scripts.ingestions.weather_raw as weather_ingestion
import scripts.utils.dq_ingestion_utils as idq

def run_weather_job(root_path, spark):
    weather_idq = idq.IngestionDataQuality()

    csv_files, weather_raw_df = weather_ingestion.load_weather_data(root_path, spark)

    # DQ ingestion: verify combined row counts of all csv files == raw dataset total row count
    weather_idq.verify_source_file_row_counts(csv_files)
    weather_idq.log_raw_df_count(weather_raw_df)
    weather_idq.verify_total_row_counts()
