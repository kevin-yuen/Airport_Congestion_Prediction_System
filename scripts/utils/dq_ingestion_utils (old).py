import pandas as pd


class IngestionDataQuality:
    def __init__(self):
        self.total_src_cnt = 0
        self.total_raw_df_cnt = 0


    def __accumulate_source_row_count(self, src_row_count):
        self.total_src_cnt += src_row_count


    def __accumulate_raw_df_row_count(self, raw_df_row_count):
        self.total_raw_df_cnt += raw_df_row_count


    def __get_source_csv_row_count(self, file_path):
        try:
            pd_df = pd.read_csv(file_path)
            row_count = len(pd_df)
            
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
            print(f"""Row count check passed!\ntotal source file: {self.total_src_cnt}\ntotal raw dataset: {self.total_raw_df_cnt}\n""")
        else:
            raise ValueError(f"""Row count check failed!\ntotal source file: {self.total_src_cnt}\ntotal raw dataset: {self.total_raw_df_cnt}\n""")


    def verify_source_file_row_counts(self, file_paths):
        file_row_counts = {}

        # get row count for each source csv file
        for file_path in file_paths:
            file_name = file_path.split("/")[-1]

            src_row_count = self.__get_source_csv_row_count(file_path)
            file_row_counts[file_name] = src_row_count

        for file_name, row_count in file_row_counts.items():
            print(f"""Source file: {file_name}\nrow count: {row_count}\n""")


    def log_raw_df_count(self, raw_df):
        row_count = self.__get_raw_df_row_count(raw_df)
        print(f"Raw dataset count: {row_count}")


    def verify_total_row_counts(self):
        print(f"Total source file row count: {self.total_src_cnt}")
    
        self.__verify_row_counts()
