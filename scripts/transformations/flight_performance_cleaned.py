import pyspark.sql.functions as F
import constants as C
from itertools import chain


def select_and_rename_columns(df, columns, column_mapping):
    return df.select(*columns).withColumnsRenamed(column_mapping)


def conditional_impute(df, condition, base_column, new_column):
    avg_duration_df = (
        df
        .filter(condition)
        .groupBy("origin_iata_code", "destination_iata_code")
        .agg(
            F.avg(base_column).alias(new_column)
        )
    )

    joined_df = (
        df
        .join(
            avg_duration_df,
            on=["origin_iata_code", "destination_iata_code"],
            how="left"
        )
    )

    imputed_df = (
        joined_df
        .withColumn(
            base_column,
            F.when(
                F.col(base_column).isNull(),
                F.round(F.col(new_column))
            ).otherwise(F.col(base_column))
        )
        .drop(new_column)
    )

    return imputed_df
