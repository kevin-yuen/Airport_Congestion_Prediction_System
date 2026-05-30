import scripts.utils.utils as u


def load_airport_data(incoming_path, spark):
    csv_files = u.list_files(
        spark,
        incoming_path,
        ".csv"
    )

    airport_files = [fp for fp in csv_files if "T_MASTER_COORD.csv" in fp]

    if not airport_files:
        raise FileNotFoundError("Airport file not found")

    airport_file_path = airport_files[0]

    # load the master raw airport coordinates data into spark dataset
    raw_df = u.load_raw_data(airport_file_path, spark)
   
    return airport_files, raw_df
