class IngestionDataQuality:
    def __init__(self):
        self.total_src_cnt = 0
        self.total_raw_df_cnt = 0


    def __accumulate_source_row_count(self, src_row_count):
        self.total_src_cnt += src_row_count


    def __accumulate_raw_df_row_count(self, raw_df_row_count):
        self.total_raw_df_cnt += raw_df_row_count


    def __get_source_csv_row_count(self, file_path, spark):
        try:
            df = (
                spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(file_path)
            )

            row_count = df.count()

            self.__accumulate_source_row_count(row_count)

            return row_count
        except Exception as e:
            filename = file_path.split("/")[-1]

            raise ValueError(f"Error counting rows in CSV file {filename}: {e}")


    def __get_raw_df_row_count(self, df):
        row_count = df.count()

        self.__accumulate_raw_df_row_count(row_count)

        return row_count


    def __verify_row_counts(self):
        if self.total_src_cnt == self.total_raw_df_cnt:
            print(
                f"Row count check passed!\n"
                f"total source file: {self.total_src_cnt}\n"
                f"total raw dataset: {self.total_raw_df_cnt}\n"
            )
        else:
            raise ValueError(
                f"Row count check failed!\n"
                f"total source file: {self.total_src_cnt}\n"
                f"total raw dataset: {self.total_raw_df_cnt}\n"
            )


    def verify_source_file_row_counts(
            self,
            file_paths,
            spark
        ):
        file_row_counts = {}

        for file_path in file_paths:
            file_name = file_path.split("/")[-1]
            src_row_count = self.__get_source_csv_row_count(file_path, spark)

            file_row_counts[file_name] = src_row_count

        for file_name, row_count in file_row_counts.items():
            print(
                f"Source file: {file_name}\n"
                f"row count: {row_count}\n"
            )


    def log_raw_df_count(self, raw_df):
        row_count = self.__get_raw_df_row_count(raw_df)

        print(f"Raw dataset count: {row_count}")


    def verify_total_row_counts(self):
        print(f"Total source file row count: {self.total_src_cnt}")

        self.__verify_row_counts()
