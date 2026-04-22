import os


ENV = os.getenv("ENV", "local")
root_path = "data/source"

CONFIG = {
    "local": {
        "airport_master": f"{root_path}/airport/",
        "airport_dept_performance": f"{root_path}/airport departure performance/",
        "flight_performance": f"{root_path}/flight performance/",
        "tsa_throughput": f"{root_path}/tsa customer throughput/",
        "tsa_wait_time": f"{root_path}/tsa wait time/",
        "weather": f"{root_path}/weather/"
    },
    "prod": {
        "input_path": "s3a://airport-congestion-prediction-system/raw/airport/T_MASTER_COORD.csv",
        "airport_dept_performance": "",
        "flight_performance": "",
        "tsa_throughput": "",
        "tsa_wait_time": "",
        "weather": ""
    }
}
