from config import ENV, CONFIG
import os

import scripts.utils.spark_session as s

import scripts.jobs.airport as airport_job
import scripts.jobs.dept_performance as dept_performance_job
import scripts.jobs.flight_performance as flight_performance_job
import scripts.jobs.tsa_throughput as tsa_throughput_job
import scripts.jobs.tsa_wait_time as tsa_wait_time_job
import scripts.jobs.weather as weather_job


def _generate_folder_paths(root_path):
    incoming_path = os.path.join(root_path, "incoming/")
    archived_path = os.path.join(root_path, "archived/")
    
    return incoming_path, archived_path


def main():
    spark = s.get_spark()
    env_config = CONFIG[ENV]

    # set up folder paths
    airport_source_path = env_config["airport_source_path"]
    airport_incoming_path, airport_archived_path = _generate_folder_paths(airport_source_path)

    dept_performance_source_path = env_config["airport_departure_performance_source_path"]
    dept_performance_incoming_path, dept_performance_archived_path = _generate_folder_paths(dept_performance_source_path)

    flight_performance_source_path = env_config["flight_performance_source_path"]
    flight_performance_incoming_path, flight_performance_archived_path = _generate_folder_paths(flight_performance_source_path)
    flight_performance_raw_parquet_path = env_config["flight_performance_raw_parquet_path"]
    flight_performance_transformed_parquet_path = env_config["flight_performance_transformed_parquet_path"]

    tsa_throughput_source_path = env_config["tsa_throughput_source_path"]
    tsa_throughput_incoming_path, tsa_throughput_archived_path = _generate_folder_paths(tsa_throughput_source_path)
    tsa_throughput_raw_parquet_path = env_config["tsa_throughput_raw_parquet_path"]
    tsa_throughput_transformed_parquet_path = env_config["tsa_throughput_transformed_parquet_path"]


    # tsa_wait_time_root_path = CONFIG[ENV]["tsa_wait_time"]
    # weather_root_path = CONFIG[ENV]["weather"]


    # run pipelines
    # airport_job.run_airport_job(airport_incoming_path, airport_archived_path, spark)

    # dept_performance_job.run_departure_performance_job(
    #     dept_performance_incoming_path, 
    #     dept_performance_archived_path, 
    #     spark)
    
    # flight_performance_job.run_flight_performance_incremental_job(
    #     flight_performance_incoming_path,
    #     flight_performance_archived_path,
    #     spark,
    #     flight_performance_raw_parquet_path,
    #     flight_performance_transformed_parquet_path)

    tsa_throughput_job.run_tsa_throughput_incremental_job(
        tsa_throughput_incoming_path,
        tsa_throughput_archived_path, 
        spark,
        tsa_throughput_raw_parquet_path,
        tsa_throughput_transformed_parquet_path)

    # tsa_wait_time_job.run_tsa_wait_time_job(tsa_wait_time_root_path, spark)
    # weather_job.run_weather_job(weather_root_path, spark)


if __name__ == "__main__":
    main()