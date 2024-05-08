import os
import pandas as pd

def partitioning(filename, chunk_size, partitioning_key1, partitioning_key2, file_path, mode, encoding=None):
    file_extension = filename.split('.')[-1]
    if file_extension == 'csv':
        reader = pd.read_csv
    elif file_extension == 'xlsx':
        reader = pd.read_excel
    else:
        raise ValueError("Unsupported file extension")

    distinct_list1 = set()
    for chunk in reader(filename, chunksize=chunk_size, encoding=encoding):
        distinct_list1.update(chunk[partitioning_key1].unique())

        for value1 in distinct_list1:
            filtered_df_by_key1 = chunk[chunk[partitioning_key1] == value1]
            distinct_list2 = set(filtered_df_by_key1[partitioning_key2].unique())

            for value2 in distinct_list2:
                filtered_df_by_key2 = filtered_df_by_key1[filtered_df_by_key1[partitioning_key2] == value2]

                os.makedirs(file_path, exist_ok=True)
                output_filename = f"{file_path}{value1}_{value2}_file.{file_extension}"
                file_mode = 'a' if mode == 'a' and os.path.exists(output_filename) else 'w'
                write_file(filtered_df_by_key2, output_filename, file_mode, file_extension, encoding)

                yield filtered_df_by_key2

def write_file(df, path, mode, file_extension, encoding=None):
    if file_extension == 'csv':
        df.to_csv(path, mode=mode, index=False, encoding=encoding)
    elif file_extension == 'xlsx':
        df.to_excel(path, index=False)
