from config import ENV, CONFIG
import scripts.utils.utils as u 


input_path = CONFIG[ENV]["airport_master"]

airport_raw_df = u.load_raw_data(input_path, 'T_MASTER_COORD.csv')
airport_raw_df.show(5)
