import scripts.utils.utils as u
import pandas as pd
import pdfplumber
from pyspark.sql.types import StructType, StructField, StringType


def __stream_excel_rows(file_path):
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


def __stream_pdf_table_rows(file_path):
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


def verify_throughput_row_counts(pdf_df, excel_df, raw_df):
    pdf_df_count = pdf_df.count()
    excel_df_count = excel_df.count()

    total_count = pdf_df_count + excel_df_count

    if total_count == raw_df.count():
        print(f"""Row count check passed!\nTotal PDF rows loaded: {pdf_df_count}\nTotal Excel rows loaded: {excel_df_count}\nTotal rows loaded (PDF + Excel): {total_count}\nTSA Throughput raw dataset row count: {raw_df.count()}""")
    else:
        raise ValueError(f"""Row count check failed!\nTotal PDF rows loaded: {pdf_df_count}\nTotal Excel rows loaded: {excel_df_count}\nTotal rows loaded (PDF + Excel): {total_count}\nTSA Throughput raw dataset row count: {raw_df.count()}""")


def load_tsa_throughput_data(root_path, spark):
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

    pdf_files, excel_files, _ = u.get_files_by_year(root_path, 2013, 2017)

    # parallelize PDF processing
    pdf_rows_rdd = u.parallelize_processing(pdf_files, __stream_pdf_table_rows, spark)
    pdf_df = spark.createDataFrame(pdf_rows_rdd, schema=schema)

    # parallelize Excel processing
    excel_rows_rdd = u.parallelize_processing(excel_files, __stream_excel_rows, spark)
    excel_df = spark.createDataFrame(excel_rows_rdd, schema=schema)
    
    if excel_df.count() > 0:
        raw_df = pdf_df.unionByName(excel_df)
    else:
        raw_df = pdf_df

    return pdf_df, excel_df, raw_df


# TODO:
# 1. clean date
# 2. write to partitioned csv