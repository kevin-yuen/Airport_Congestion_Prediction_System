import pyspark.sql.functions as F
import constants as C
from itertools import chain


def remove_nulls(df, column):
    return df.filter(F.isnotnull(F.col(column)))


def clean_airport_location(df):
    split_expr = F.split(
        F.regexp_replace(
            F.col("airport_location"),
            r"\s*\(|\)",
            ", "
        ),
        ", "
    )

    df = df.withColumn('city_name', F.element_at(split_expr, 1)) \
            .withColumn('state_code', F.element_at(split_expr, 2)) \
            .withColumn('iata_code', F.element_at(split_expr, 3))
    
    return df


def map_state_name(df):
    mapping_expr = F.create_map(
        [F.lit(x) for x in chain(*C.STATES.items())]
    )

    df = df.withColumn('state_name', mapping_expr[F.col('state_code')])
    return df


def rename_and_select_columns(df, col_mapping, cols):
    return df.withColumnsRenamed(col_mapping).select(*cols)


def get_data_by_year(df, column, start_year, end_year):
    return df.filter((F.col(column) >= start_year) & (F.col(column) <= end_year))
