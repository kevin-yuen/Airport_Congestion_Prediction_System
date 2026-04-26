from config import ENV, CONFIG
import scripts.utils.utils as u 


root_path = CONFIG[ENV]["airport_master"]

airport_raw = u.load_raw_data(root_path, 'T_MASTER_COORD.csv')
airport_raw.show(5)
