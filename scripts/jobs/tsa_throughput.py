import scripts.ingestions.tsa_throughput_raw as tsa_throughput_ingestion

def run_tsa_throughput_job(root_path, spark):
    pdf_df, excel_df, throughput_raw_df = tsa_throughput_ingestion.load_tsa_throughput_data(root_path, spark)

    # DQ ingestion: verify pdf row counts == excel row counts ==  raw dataset total row count
    tsa_throughput_ingestion.verify_throughput_row_counts(pdf_df, excel_df, throughput_raw_df)
    