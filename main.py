from config import ENV, CONFIG
import os

import scripts.utils.spark_session as s

import scripts.jobs.airport as airport_job
import scripts.jobs.dept_performance as dept_performance_job
import scripts.jobs.flight_performance as flight_performance_job
import scripts.jobs.tsa_throughput as tsa_throughput_job
import scripts.jobs.tsa_wait_time as tsa_wait_time_job
import scripts.jobs.weather as weather_job
import scripts.jobs.airport_weather_mapping as aw_mapping_job


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
    airport_transformed_csv_path = env_config["airport_transformed_csv_path"]

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

    tsa_wait_time_source_path = env_config["tsa_wait_time_source_path"]
    tsa_wait_time_incoming_path, tsa_wait_time_archived_path = _generate_folder_paths(tsa_wait_time_source_path)

    weather_source_path = env_config["weather_source_path"]
    weather_incoming_path = os.path.join(weather_source_path, "incoming/")
    weather_archived_path = os.path.join(weather_source_path, "incoming_archived/")
    weather_raw_parquet_path = env_config["weather_raw_parquet_path"]
    weather_transformed_parquet_path = env_config["weather_transformed_parquet_path"]


    # run pipelines
    # airport_job.run_airport_job(
    #     airport_incoming_path, 
    #     airport_archived_path, 
    #     airport_transformed_csv_path, 
    #     spark)

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

    # tsa_throughput_job.run_tsa_throughput_incremental_job(
    #     tsa_throughput_incoming_path,
    #     tsa_throughput_archived_path, 
    #     spark,
    #     tsa_throughput_raw_parquet_path,
    #     tsa_throughput_transformed_parquet_path)

    # tsa_wait_time_job.run_tsa_wait_time_job(
    #     tsa_wait_time_incoming_path,
    #     tsa_wait_time_archived_path,
    #     spark)

    # weather_job.run_tsa_weather_incremental_job(
    #     weather_source_path,
    #     weather_incoming_path,
    #     weather_archived_path, 
    #     spark,
    #     weather_raw_parquet_path,
    #     weather_transformed_parquet_path)

    aw_mapping_job.run_airport_weather_mapping_job(
        spark,
        airport_transformed_csv_path,
        weather_transformed_parquet_path
    )


if __name__ == "__main__":
    main()