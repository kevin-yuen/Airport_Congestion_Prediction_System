import scripts.utils.utils as u
import pyspark.sql.functions as F


def select_columns(df, columns):
    return u.select_columns(df, columns)


def rename_columns(df, col_mapping):
    return u.rename_columns(df, col_mapping)


def get_active_airport(df):
    return df.filter((df['is_closed'] == 0) & (df['is_latest'] == 1))


def clean_city(df):
    airport_clean_city_df = df.withColumn(
            'city',
            F.element_at(F.split(F.col('city'), ', '), 1)
        ).filter(F.col('iata_code') != 'ZZZ')       # 'ZZZ' = Unknown airport

    # 'CVG' iata_code, replace state_code with 'OH'
    # 'WAS' iata_code, replace state_code with 'VA'
    airport_clean_city_df = airport_clean_city_df.withColumn(
        'state_code',
        F.when(F.col('iata_code') == 'CVG', 'OH')
            .when(F.col('iata_code') == 'WAS', 'VA')
            .otherwise(F.col('state_code'))
    )

    airport_clean_city_df = airport_clean_city_df.withColumn(
        'state_name',
        F.when(F.col('iata_code') == 'WAS', 'Virginia').otherwise(F.col('state_name'))
    )

    return airport_clean_city_df


def get_active_BER(df):
    # 2 active BER airports found. Hence, use close_date to determine the final active airport
    non_BER_df = df.filter((F.col('iata_code') != 'BER'))
    active_BER_df = df.filter((F.col('iata_code') == 'BER') & (F.isnull(F.col('close_date'))))

    df = non_BER_df.unionByName(active_BER_df)
    return df
