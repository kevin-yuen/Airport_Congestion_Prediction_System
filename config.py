import os


ENV = os.getenv("ENV", "local")

CONFIG = {
    "local": {
        "input_path": "data/source/airport/T_MASTER_COORD.csv"
    },
    "prod": {
        "input_path": "s3a://airport-congestion-prediction-system/raw/airport/T_MASTER_COORD.csv"
    }
}
