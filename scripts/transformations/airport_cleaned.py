import pyspark.sql.functions as F


def select_and_rename_columns(df, columns, column_mapping):
    return df.select(*columns).withColumnsRenamed(column_mapping)


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


def get_active_BER_airport(df):
    # 2 active BER airports found. Hence, use close_date to determine the final active airport
    non_BER_df = df.filter((F.col('iata_code') != 'BER'))
    active_BER_df = df.filter((F.col('iata_code') == 'BER') & (F.isnull(F.col('close_date'))))

    return non_BER_df.unionByName(active_BER_df)


def clean_dates(df, columns):
    for column in columns:
        df = df.withColumn(f"{column}_ts", F.to_timestamp(column, "M/d/yyyy H:mm")) \
                .withColumn(f"{column}_date", F.to_date(F.col(f"{column}_ts"))) \
                .withColumn(f"{column}_year", F.year(F.col(f"{column}_date"))) \
                .withColumn(f"{column}_year_month", F.col(f"{column}_ts").substr(1, 7))

    return df


def get_data_by_year(df, max_year):
    return df.filter(F.col('start_date_year') <= max_year)


def drop_and_rename_columns(df, columns, column_mapping):
    return df.drop(*columns).withColumnsRenamed(column_mapping)