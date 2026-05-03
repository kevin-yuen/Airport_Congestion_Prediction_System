from config import ENV, CONFIG
import scripts.utils.spark_session as s
import scripts.jobs.airport as airport_job
import scripts.jobs.dept_performance as dept_performance_job
import scripts.jobs.flight_performance as flight_performance_job
import scripts.jobs.tsa_throughput as tsa_throughput_job
import scripts.jobs.tsa_wait_time as tsa_wait_time_job
import scripts.jobs.weather as weather_job


def main():
    spark = s.get_spark()

    airport_root_path = CONFIG[ENV]["airport_master"]
    dept_performance_root_path = CONFIG[ENV]["airport_dept_performance"]
    flight_performance_root_path = CONFIG[ENV]["flight_performance"]
    tsa_throughput_root_path = CONFIG[ENV]["tsa_throughput"]
    tsa_wait_time_root_path = CONFIG[ENV]["tsa_wait_time"]
    weather_root_path = CONFIG[ENV]["weather"]

    airport_job.run_airport_job(airport_root_path, spark)
    # dept_performance_job.run_departure_performance_job(dept_performance_root_path, spark)
    # flight_performance_job.run_flight_performance_job(flight_performance_root_path, spark)
    # tsa_throughput_job.run_tsa_throughput_job(tsa_throughput_root_path, spark)
    # tsa_wait_time_job.run_tsa_wait_time_job(tsa_wait_time_root_path, spark)
    # weather_job.run_weather_job(weather_root_path, spark)


if __name__ == "__main__":
    main()