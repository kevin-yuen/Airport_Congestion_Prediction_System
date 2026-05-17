import pyspark.sql.functions as F
from pyspark.sql import Window


def convert_nan_to_null(df):
    for col in df.columns:
        df = df.withColumn(
            col,
            F.when(F.col(col) == "NaN", None).otherwise(F.col(col))
        )
    
    return df


def ffill_nulls(df):
    df = df.withColumn("row_num", F.monotonically_increasing_id())

    w = Window.orderBy("row_num").rowsBetween(Window.unboundedPreceding, 0)
    df_filled = df.withColumns({
        "iata_code": F.last("iata_code", ignorenulls=True).over(w),
        "airport": F.last("airport", ignorenulls=True).over(w)
    }).drop("row_num", "col_1")

    return df_filled


def rename_col_columns(df):
    target_columns = df.columns[3:]
    rename_mapping = {}
    column_index = 0

    # -----------------------------------
    # DECEMBER -> JUNE
    # 10 years: 2015 -> 2006
    # -----------------------------------
    for month in range(12, 5, -1):
        for year in range(2015, 2005, -1):
            old_col = target_columns[column_index]
            new_col = f"{year}-{month:02d}"

            rename_mapping[old_col] = new_col

            column_index += 1

    # -----------------------------------
    # MAY -> JANUARY
    # 11 years: 2016 -> 2006
    # -----------------------------------
    for month in range(5, 0, -1):
        for year in range(2016, 2005, -1):

            old_col = target_columns[column_index]
            new_col = f"{year}-{month:02d}"

            rename_mapping[old_col] = new_col

            column_index += 1

    # -----------------------------------
    # APPLY RENAMES
    # -----------------------------------
    for old_col, new_col in rename_mapping.items():
        df = df.withColumnRenamed(
            old_col,
            new_col
        )
    
    return df


def convert_wide_to_narrow(df):
    id_columns = ["iata_code", "airport", "checkpoint"]
    value_columns = df.columns[3:]

    stack_expr = ", ".join([f"'{c}', `{c}`" for c in value_columns])
    narrow_df = df.select(
        *id_columns,
        F.expr(
            f"""
            stack(
                {len(value_columns)},
                {stack_expr}
            ) as (year_month, wait_time)
            """
        )
    )

    return narrow_df