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

TRANSFORMED_ROOT_PATH = os.path.join(
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
    "weather_raw_parquet_path": os.path.join(
        RAW_PARQUET_ROOT_PATH,
        "weather_throughput"
    ),

    # transformed parquet output
    "flight_performance_transformed_parquet_path": os.path.join(
        TRANSFORMED_ROOT_PATH,
        "flight_performance"
    ),
    "tsa_throughput_transformed_parquet_path": os.path.join(
        TRANSFORMED_ROOT_PATH,
        "tsa_customer_throughput"
    ),
    "weather_transformed_parquet_path": os.path.join(
        TRANSFORMED_ROOT_PATH,
        "weather_throughput"
    ),

    # transformed csv output
    "airport_transformed_csv_path": os.path.join(
        TRANSFORMED_ROOT_PATH,
        "airport"
    ),
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

    "weather_raw_parquet_path":
        "s3a://airport-congestion-prediction-system/output/raw/weather/",

    # transformed parquet
    "flight_performance_transformed_parquet_path":
        "s3a://airport-congestion-prediction-system/output/transformed/flight_performance/",

    "tsa_throughput_transformed_parquet_path":
        "s3a://airport-congestion-prediction-system/output/transformed/tsa_customer_throughput/",

    "weather_transformed_parquet_path":
        "s3a://airport-congestion-prediction-system/output/transformed/weather/"
}


# -----------------------------
# Environment Config
# -----------------------------
CONFIG = {
    "local": LOCAL_CONFIG,
    "prod": PROD_CONFIG
}
