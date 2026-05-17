import scripts.ingestions.tsa_wait_time_raw as wt_ingestion
import scripts.transformations.tsa_wait_time_cleaned as wt_transformation
import scripts.utils.dq_transformation_utils as dq_transformation
import scripts.utils.utils as u

import pyspark.sql.functions as F
import os

def run_tsa_wait_time_job(incoming_path, archived_path, spark):
    # ---------------------------------------------------
    # [START] INGESTION
    # ---------------------------------------------------
    # ingest raw data for TSA wait time
    pd_df, wt_raw_df = wt_ingestion.load_tsa_wait_time_data(incoming_path, spark)

    # DQ ingestion: verify source row counts raw dataset total row count
    wt_ingestion.verify_wait_time_row_counts(pd_df, wt_raw_df)
    # ---------------------------------------------------
    # [END] INGESTION
    # ---------------------------------------------------

    file_path = os.path.join(incoming_path, 'tsa-wait-times-january-2006-december-2015.xls')
    u.move_file_to_archived(file_path, archived_path)

    # ---------------------------------------------------
    # [START] DQ CHECK BEFORE TRANSFORMATION
    # ---------------------------------------------------
    print("""\n***********************************************
*********** DQ BEFORE TRANSFORMATION ***********
************************************************\n""")
    # schema check
    wt_raw_df.printSchema()
    # null check
    dq_transformation.check_null_counts(wt_raw_df)
    # uniqueness check
    dq_transformation.check_column_uniqueness(
        wt_raw_df,
        wt_raw_df.columns
    )
    # sample data check
    wt_raw_df.show(20, truncate=False)
    # sample data check for forward-filling
    wt_raw_df.filter(wt_raw_df["checkpoint"].isin(
        "AUS01",
        "AUS02",
        "AUS03",
        "AUS04"
    )).select("iata_code", "airport", "checkpoint").show(20)
    # ---------------------------------------------------
    # [END] DQ CHECK BEFORE TRANSFORMATION
    # ---------------------------------------------------


    # ---------------------------------------------------
    # [START] TRANSFORMATION
    # ---------------------------------------------------
    # convert string "NaN" to actual null
    df = wt_transformation.convert_nan_to_null(wt_raw_df)

    # forward-fill nulls for iata_code and airport
    df_filled = wt_transformation.ffill_nulls(df)

    # rename col_* for converting dataset from wide to narrow
    df_renamed = wt_transformation.rename_col_columns(df_filled)

    # convert dataset from wide to narrow
    df_narrow = wt_transformation.convert_wide_to_narrow(df_renamed)

    df_narrow = df_narrow.withColumns({
        "year": F.substring("year_month", 1, 4).cast("int"),
        "month": F.substring("year_month", 6, 2).cast("int")
    })

    columns = ["iata_code", "checkpoint", "month", "year", "airport", "avg_wait_time_minutes"]
    df_cleaned = (
        df_narrow
        .withColumnRenamed(
            "wait_time", 
            "avg_wait_time_minutes"
        )
        .withColumn(
            "avg_wait_time_minutes", 
            F.col("avg_wait_time_minutes").cast("integer")
        )
        .select(*columns)
    )
    # ---------------------------------------------------
    # [END] TRANSFORMATION
    # ---------------------------------------------------


    # ---------------------------------------------------
    # [START] DQ CHECK AFTER TRANSFORMATION
    # ---------------------------------------------------
    print("""\n***********************************************
*********** DQ AFTER TRANSFORMATION ***********
************************************************\n""")
    # sample data check
    df_cleaned.show(10, truncate=False)
    # final schema check
    df_cleaned.printSchema()
    # null check
    dq_transformation.check_null_counts(df_cleaned)
    # ---------------------------------------------------
    # [END] DQ CHECK AFTER TRANSFORMATION
    # ---------------------------------------------------
