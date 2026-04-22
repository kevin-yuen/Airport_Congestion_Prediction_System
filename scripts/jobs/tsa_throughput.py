# import pyspark.sql.functions as F
# from config import ENV, CONFIG
# import scripts.utils.utils as u


# def load_raw_throughput():
#     base_path = CONFIG[ENV]["tsa_throughput"]

#     raw_df = u.load_partitioned_data_by_year(
#         base_path,
#         2013,
#         2017,
#         [".xlsx", ".pdf"]
#     )
    
#     return raw_df


# tsa_throughput_raw_df = load_raw_throughput()
# tsa_throughput_raw_df.show(5)
