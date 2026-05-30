import os


# ENV = os.getenv("ENV", "local")
ENV = os.getenv("ENV", "prod")

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
        "weather"
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
    "tsa_wait_time_transformed_parquet_path": os.path.join(
        TRANSFORMED_ROOT_PATH,
        "tsa_wait_time"
    ),
    "weather_transformed_parquet_path": os.path.join(
        TRANSFORMED_ROOT_PATH,
        "weather"
    ),
    "gold_transformed_parquet_path": os.path.join(
        TRANSFORMED_ROOT_PATH,
        "airport_congestion_gold"
    ),

    # transformed csv output
    "airport_transformed_csv_path": os.path.join(
        TRANSFORMED_ROOT_PATH,
        "airport"
    ),
    "airport_departure_performance_transformed_csv_path": os.path.join(
        TRANSFORMED_ROOT_PATH,
        "airport_departure_performance"
    ),
    "airport_weather_mapping_transformed_csv_path": os.path.join(
        TRANSFORMED_ROOT_PATH,
        "airport_weather_mapping"
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

    # "flight_performance_source_path":
    #     "s3a://airport-congestion-prediction-system/source/flight_performance/",

    "flight_performance_source_path": os.path.join(
        SOURCE_ROOT_PATH,
        "flight_performance"
    ),

    # "tsa_throughput_source_path":
    #     "s3a://airport-congestion-prediction-system/source/tsa_customer_throughput/",

    "tsa_throughput_source_path": os.path.join(
        SOURCE_ROOT_PATH,
        "tsa_customer_throughput"
    ),

    "tsa_wait_time_source_path":
        "s3a://airport-congestion-prediction-system/source/tsa_wait_time/",

    # "weather_source_path":
    #     "s3a://airport-congestion-prediction-system/source/weather/",

    "weather_source_path": os.path.join(
        SOURCE_ROOT_PATH,
        "weather"
    ),

    # raw parquet
    # "flight_performance_raw_parquet_path":
    #     "s3a://airport-congestion-prediction-system/output/raw/flight_performance/",

    "flight_performance_raw_parquet_path": os.path.join(
        RAW_PARQUET_ROOT_PATH,
        "flight_performance"
    ),

    # "tsa_throughput_raw_parquet_path":
    #     "s3a://airport-congestion-prediction-system/output/raw/tsa_customer_throughput/",

    "tsa_throughput_raw_parquet_path": os.path.join(
        RAW_PARQUET_ROOT_PATH,
        "tsa_customer_throughput"
    ),

    # "weather_raw_parquet_path":
    #     "s3a://airport-congestion-prediction-system/output/raw/weather/",

    "weather_raw_parquet_path": os.path.join(
        RAW_PARQUET_ROOT_PATH,
        "weather"
    ),

    # transformed parquet
    # "flight_performance_transformed_parquet_path":
    #     "s3a://airport-congestion-prediction-system/output/transformed/flight_performance/",

    "flight_performance_transformed_parquet_path": os.path.join(
        TRANSFORMED_ROOT_PATH,
        "flight_performance"
    ),

    # "tsa_throughput_transformed_parquet_path":
    #     "s3a://airport-congestion-prediction-system/output/transformed/tsa_customer_throughput/",

    "tsa_throughput_transformed_parquet_path": os.path.join(
        TRANSFORMED_ROOT_PATH,
        "tsa_customer_throughput"
    ),

    "tsa_wait_time_transformed_parquet_path":
        "s3a://airport-congestion-prediction-system/output/transformed/tsa_wait_time/",

    # "weather_transformed_parquet_path":
    #     "s3a://airport-congestion-prediction-system/output/transformed/weather/",

    "weather_transformed_parquet_path": os.path.join(
        TRANSFORMED_ROOT_PATH,
        "weather"
    ),
    
    "gold_transformed_parquet_path":
        "s3a://airport-congestion-prediction-system/output/transformed/airport_congestion_gold/",

    # transformed csv
    "airport_transformed_csv_path":
        "s3a://airport-congestion-prediction-system/output/transformed/airport/",

    "airport_departure_performance_transformed_csv_path":
        "s3a://airport-congestion-prediction-system/output/transformed/airport_departure_performance/",

    "airport_weather_mapping_transformed_csv_path":
        "s3a://airport-congestion-prediction-system/output/transformed/airport_weather_mapping/",
}


# -----------------------------
# Environment Config
# -----------------------------
CONFIG = {
    "local": LOCAL_CONFIG,
    "prod": PROD_CONFIG
}
