from config import ENV, CONFIG
import scripts.utils.utils as u
import pandas as pd
import pdfplumber
from pyspark.sql.types import StructType, StructField, StringType


def process_excel(file_path):
    try:
        pd_df = pd.read_excel(file_path, skiprows=2, header=None)
        pd_df.columns = [
            'date', 
            'hour',
            'iata_code',
            'airport',
            'city', 
            'state', 
            'checkpoint', 
            'metrics', 
            'total_customer_throughput'
        ]

        pd_df = pd_df.astype(str)

        for row in pd_df.itertuples(index=False, name=None):
            yield row
    except Exception as e:
        filename = file_path.split("/")[-1]
        print(f"Error processing Excel file {filename}: {e}")


def process_pdf(file_path):
    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages[:3]:         # test with first 3 pages to avoid long processing time during development
            # for page in pdf.pages:
                table = page.extract_table()

                if table:
                    for row in table[1:]:     # skip header row
                        if not row:
                            continue

                        casted_row =[str(val).strip() if val is not None else None for val in row]
                        yield tuple(casted_row)
    except Exception as e:
        filename = file_path.split("/")[-1]
        print(f"Error processing PDF file {filename}: {e}")


# def parallelize_processing(files, process_function):
#     num_cores = spark.sparkContext.defaultParallelism       # get the number of available cores
#     num_partitions = num_cores * 2                          # set number of partitions to 2x the number of cores

#     rdd = spark.sparkContext.parallelize(files, num_partitions)
#     rows_rdd = rdd.flatMap(process_function)

#     return rows_rdd


def load_raw_tsa_throughput():
    base_path = CONFIG[ENV]["tsa_throughput"]
    spark = u.get_spark()

    schema = StructType([
        StructField('date', StringType(), True),
        StructField('hour', StringType(), True),
        StructField('iata_code', StringType(), True),
        StructField('airport', StringType(), True),
        StructField('city', StringType(), True),
        StructField('state', StringType(), True),
        StructField('checkpoint', StringType(), True),
        StructField('metrics', StringType(), True),
        StructField('total_customer_throughput', StringType(), True)
    ])

    pdf_files, excel_files, _ = u.get_files_by_year(base_path, 2013, 2015)

    # parallelize PDF processing
    pdf_rows_rdd = u.parallelize_processing(pdf_files, process_pdf)
    pdf_df = spark.createDataFrame(pdf_rows_rdd, schema=schema)

    # parallelize Excel processing
    excel_rows_rdd = u.parallelize_processing(excel_files, process_excel)
    excel_df = spark.createDataFrame(excel_rows_rdd, schema=schema)
    
    if excel_df.count() > 0:
        tsa_throughput_df = pdf_df.unionByName(excel_df)
    else:
        tsa_throughput_df = pdf_df

    print(f"Total PDF rows: {pdf_df.count()}")
    print(f"Total Excel rows: {excel_df.count()}")

    return tsa_throughput_df


# def write_partitioned_cssv(df, output_path):
#     df.write.partitionBy("date").mode("overwrite").option("header", "true").csv(output_path)


tsa_throughput_raw = load_raw_tsa_throughput()

# TODO:
# 1. clean date
# 2. write to partitioned csv