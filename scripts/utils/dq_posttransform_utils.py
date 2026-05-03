def check_column_uniqueness(df, column):
    distinct_count_iata = df.select(column).distinct().count()
    print(distinct_count_iata == df.count())

