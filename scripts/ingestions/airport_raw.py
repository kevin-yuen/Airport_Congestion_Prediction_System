import scripts.utils.utils as u


def load_airport_data(root_path, spark):
    file_path = root_path + "T_MASTER_COORD.csv"
    csv_files = [file_path]

    # load the master raw airport coordinates data into spark dataset
    raw_df = u.load_raw_data(file_path, spark)

    return csv_files, raw_df
