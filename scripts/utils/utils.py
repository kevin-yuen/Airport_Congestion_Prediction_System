import os
import shutil
import glob
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def list_files(
        spark,
        directory_path,
        extension=None
    ):
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(directory_path)
    fs = path.getFileSystem(hadoop_conf)

    if not fs.exists(path):
        raise FileNotFoundError(f"Directory not found: {directory_path}")

    file_statuses = fs.listStatus(path)
    files = []

    for status in file_statuses:
        file_path = status.getPath().toString()

        if file_path.startswith("file:"):
            file_path = file_path.replace("file:", "", 1)

        if extension:
            if file_path.endswith(extension):
                files.append(file_path)
        else:
            files.append(file_path)

    return files


def load_raw_data(file_path, spark, header=True):
    df = spark.read \
        .option("header", header) \
        .option("inferSchema", "true") \
        .csv(file_path)
    
    return df


def get_files_by_year(
        base_path,
        spark,
        start_year,
        end_year
    ):
    excel_files = []
    csv_files = []

    for year in range(start_year, end_year + 1):
        partition_folder = f"year={year}"
        folder_path = os.path.join(base_path, partition_folder)

        try:
            current_year_files = list_files(spark, folder_path)

            excel_files.extend([f for f in current_year_files if f.endswith(".xlsx")])
            csv_files.extend([f for f in current_year_files if f.endswith(".csv")])
        except FileNotFoundError:
            print(f"[WARNING] Missing partition: {partition_folder}")

    if not (excel_files or csv_files):
        raise FileNotFoundError("[WARNING] No files found.")

    return excel_files, csv_files


# def get_files_by_year(base_path, spark, start_year, end_year):
#     pdf_files = []
#     excel_files = []
#     csv_files = []

#     all_files = []

#     def collect_files(collector, file_paths, extension):
#         collector.extend([
#             file_path
#             for file_path in file_paths
#             if file_path.endswith(extension)
#         ])

#     for year in range(start_year, end_year + 1):
#         partition_folder = f"year={year}"
#         folder_path = os.path.join(base_path, partition_folder)
#         # current_year_files = glob.glob(os.path.join(folder_path, "*"))
#         current_year_files = list_files(spark, folder_path)

#         if current_year_files:
#             all_files.extend(current_year_files)

#     if not all_files:
#         raise FileNotFoundError("[WARNING] No files found.")
    
#     collect_files(pdf_files, all_files, ".pdf")
#     collect_files(excel_files, all_files, ".xlsx")
#     collect_files(csv_files, all_files, ".csv")

#     return pdf_files, excel_files, csv_files


def get_file_by_state(base_path):
    partition_folder_paths = glob.glob(os.path.join(base_path, "*"))

    if partition_folder_paths:
        partition_key = partition_folder_paths[0].split("=")[-1]
        
        partition_folder = f"state={partition_key}"
        folder_path = os.path.join(base_path, partition_folder)
        state_file = glob.glob(os.path.join(folder_path, "*"))[0]

    return state_file


def parallelize_processing(files, process_function, spark):
    # num_cores = spark.sparkContext.defaultParallelism       # get the number of available cores
    # num_partitions = num_cores * 2                          # set number of partitions to 2x the number of cores

    num_partitions = min(len(files), 4)

    rdd = spark.sparkContext.parallelize(files, num_partitions)
    rows_rdd = rdd.flatMap(process_function)
    return rows_rdd


def move_file_to_archived(file_path: str, archived_folder: str):
    if file_path.startswith("s3a://"):
        print("[INFO] S3 archival skipped.")
        return

    os.makedirs(archived_folder, exist_ok=True)

    file_name = os.path.basename(file_path)
    destination_path = os.path.join(archived_folder, file_name)

    shutil.move(file_path, destination_path)

    print(f"File archived to: {destination_path}")


def path_exists(spark, path: str) -> bool:
    if path.startswith("s3a://"):
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
        fs = hadoop_path.getFileSystem(hadoop_conf)
        return fs.exists(hadoop_path)
    else:
        return os.path.exists(path)


def write_partitioned_parquet(
    spark,
    df: DataFrame,
    output_path: str,
    partition_cols: list      
):
    # enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df.write.mode("overwrite").partitionBy(*partition_cols).parquet(output_path)
    print(f"[SUCCESS] Parquet written to: {output_path}")


def get_affected_partitions(incoming_df, partition):
    # get affected partitions
    affected_partitions = (
        incoming_df
        .select(partition)
        .distinct()
        .orderBy(partition)
        .rdd
        .flatMap(lambda x: x)
        .collect()
    )

    print(f"[INFO] Partitions to overwrite: {affected_partitions}")
    return affected_partitions


def archive_partition(
        incoming_root_path,
        archived_root_path,
        partition
    ):
    if incoming_root_path.startswith("s3a://"):
        print(f"[INFO] S3 archival skipped for partition: {partition}")
        return

    source_partition_path = os.path.join(incoming_root_path, partition)
    destination_partition_path = os.path.join(archived_root_path, partition)

    os.makedirs(archived_root_path, exist_ok=True)

    if os.path.exists(destination_partition_path):
        shutil.rmtree(destination_partition_path)

    shutil.move(source_partition_path, destination_partition_path)

    print(f"[ARCHIVED] {source_partition_path} -> {destination_partition_path}")

# def archive_partition(
#     incoming_root_path: str,
#     archived_root_path: str,
#     partition: str
# ):
#     source_partition_path = os.path.join(
#         incoming_root_path,
#         partition
#     )

#     destination_partition_path = os.path.join(
#         archived_root_path,
#         partition
#     )

#     # create archived root folder if not exists
#     os.makedirs(archived_root_path, exist_ok=True)

#     # remove existing archived partition if exists
#     # prevents shutil.move() conflicts
#     if os.path.exists(destination_partition_path):
#         shutil.rmtree(destination_partition_path)

#     shutil.move(
#         source_partition_path,
#         destination_partition_path
#     )

#     print(
#         f"[ARCHIVED] {source_partition_path} -> {destination_partition_path}"
#     )


def incremental_updates(spark, incoming_df, parquet_path, partition, affected_partitions):
    incoming_df = incoming_df.cache()
    
    if path_exists(spark, parquet_path):
        existing_df = (
            spark.read
            .parquet(parquet_path)
            .filter(
                F.col(partition).isin(affected_partitions)
            )
        )

        merged_df = (
            existing_df
            .unionByName(incoming_df)
            .dropDuplicates()
        )
    else:
        merged_df = incoming_df
    
    write_partitioned_parquet(spark, merged_df, parquet_path, [partition])

