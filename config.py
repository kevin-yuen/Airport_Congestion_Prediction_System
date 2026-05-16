import os


ENV = os.getenv("ENV", "local")

# -----------------------------
# Base Paths
# -----------------------------
BASE_DIR = "/app"

SOURCE_ROOT_PATH = os.path.join(
    BASE_DIR,
    "data",
    "source"
)

RAW_PARQUET_ROOT_PATH = os.path.join(
    BASE_DIR,
    "data",
    "output",
    "raw"
)

TRANSFORMED_PARQUET_ROOT_PATH = os.path.join(
    BASE_DIR,
    "data",
    "output",
    "transformed"
)


# -----------------------------
# Local Environment Config
# -----------------------------
LOCAL_CONFIG = {
    # source folders
    "airport_source_path": os.path.join(
        SOURCE_ROOT_PATH,
        "airport"
    ),

    "airport_departure_performance_source_path": os.path.join(
        SOURCE_ROOT_PATH,
        "airport_departure_performance"
    ),

    "flight_performance_source_path": os.path.join(
        SOURCE_ROOT_PATH,
        "flight_performance"
    ),

    "tsa_throughput_source_path": os.path.join(
        SOURCE_ROOT_PATH,
        "tsa_customer_throughput"
    ),

    "tsa_wait_time_source_path": os.path.join(
        SOURCE_ROOT_PATH,
        "tsa_wait_time"
    ),

    "weather_source_path": os.path.join(
        SOURCE_ROOT_PATH,
        "weather"
    ),

    # raw parquet output
    "flight_performance_raw_parquet_path": os.path.join(
        RAW_PARQUET_ROOT_PATH,
        "flight_performance"
    ),

    "tsa_throughput_raw_parquet_path": os.path.join(
        RAW_PARQUET_ROOT_PATH,
        "tsa_customer_throughput"
    ),

    # transformed parquet output
    "flight_performance_transformed_parquet_path": os.path.join(
        TRANSFORMED_PARQUET_ROOT_PATH,
        "flight_performance"
    ),

    "tsa_throughput_transformed_parquet_path": os.path.join(
        TRANSFORMED_PARQUET_ROOT_PATH,
        "tsa_customer_throughput"
    )
}


# -----------------------------
# Production Environment Config
# -----------------------------
PROD_CONFIG = {
    # source
    "airport_source_path":
        "s3a://airport-congestion-prediction-system/source/airport/",

    "airport_departure_performance_source_path":
        "s3a://airport-congestion-prediction-system/source/airport_departure_performance/",

    "flight_performance_source_path":
        "s3a://airport-congestion-prediction-system/source/flight_performance/",

    "tsa_throughput_source_path":
        "s3a://airport-congestion-prediction-system/source/tsa_customer_throughput/",

    "tsa_wait_time_source_path":
        "s3a://airport-congestion-prediction-system/source/tsa_wait_time/",

    "weather_source_path":
        "s3a://airport-congestion-prediction-system/source/weather/",

    # raw parquet
    "flight_performance_raw_parquet_path":
        "s3a://airport-congestion-prediction-system/output/raw/flight_performance/",

    "tsa_throughput_raw_parquet_path":
        "s3a://airport-congestion-prediction-system/output/raw/tsa_customer_throughput/",

    # transformed parquet
    "flight_performance_transformed_parquet_path":
        "s3a://airport-congestion-prediction-system/output/transformed/flight_performance/",

    "tsa_throughput_transformed_parquet_path":
        "s3a://airport-congestion-prediction-system/output/transformed/tsa_customer_throughput/"
}


# -----------------------------
# Environment Config
# -----------------------------
CONFIG = {
    "local": LOCAL_CONFIG,
    "prod": PROD_CONFIG
}





####################################
##################################
###############################

# import os


# ENV = os.getenv("ENV", "local")
# root_path = "/app/data/source"
# parquet_root_path = "/app/data/output/raw"
# parquet_transformed_root_path = "/app/data/output/transformed"

# CONFIG = {
#     "local": {
#         "airport_master": f"{root_path}/airport/",
#         "airport_dept_performance": f"{root_path}/airport_departure_performance/",
#         "flight_performance": f"{root_path}/flight_performance/",
#         "flight_performance_raw_parquet": f"{parquet_root_path}/flight_performance/",
#         "flight_performance_transformed_parquet": f"{parquet_transformed_root_path}/flight_performance/",
#         "tsa_throughput": f"{root_path}/tsa_customer_throughput/",
#         "tsa_throughput_raw_parquet": f"{parquet_root_path}/tsa_customer_throughput/",
#         "tsa_throughput_transformed_parquet": f"{parquet_transformed_root_path}/tsa_customer_throughput/",
#         "tsa_wait_time": f"{root_path}/tsa_wait_time/",
#         "weather": f"{root_path}/weather/"
#     },
#     "prod": {
#         "airport_master": "s3a://airport-congestion-prediction-system/raw/airport/T_MASTER_COORD.csv",
#         "airport_dept_performance": "",
#         "flight_performance": "",
#         "flight_performance_raw_parquet": "",
#         "tsa_throughput": "",
#         "tsa_wait_time": "",
#         "weather": ""
#     }
# }
