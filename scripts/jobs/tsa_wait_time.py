import scripts.ingestions.tsa_wait_time_raw as tsa_wait_time_ingestion

def run_tsa_wait_time_job(root_path, spark):
    pd_df, wait_time_raw_df = tsa_wait_time_ingestion.load_tsa_wait_time_data(root_path, spark)

    # DQ ingestion: verify source row counts raw dataset total row count
    tsa_wait_time_ingestion.verify_wait_time_row_counts(pd_df, wait_time_raw_df)