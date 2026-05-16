import scripts.utils.utils as u
import glob


def load_airport_data(incoming_path, spark):
    csv_files = glob.glob(f"{incoming_path}T_MASTER_COORD.csv")

    if not csv_files:
        raise FileNotFoundError(f"No airport files found in: {incoming_path}")

    # load the master raw airport coordinates data into spark dataset
    raw_df = u.load_raw_data(csv_files[0], spark)

    return csv_files, raw_df
