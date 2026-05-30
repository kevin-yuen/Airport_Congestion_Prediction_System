def log_ingestion_summary(file_paths, raw_df):
    print("\n--------------- INGESTION SUMMARY ---------------\n")
    print(f"Files ingested: {len(file_paths)}")

    print("\nSample source files:")
    for fp in file_paths[:5]:
        print(fp)

    print(f"\nRaw dataset row count: {raw_df.count()}")
