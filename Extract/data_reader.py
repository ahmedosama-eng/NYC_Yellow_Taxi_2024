import os

def reading_the_files_in_df(spark):
    data={}
    directory = r"F:\data project\Taxi_track_2024\data\2024"
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_name=file_path.split("\\")[5].split("_")[2].split(".")[0]
            data[file_name]=spark.read.parquet(file_path)

    return data



def reading_the_lookups(spark):
    lookups={}
    directory = r"F:\data project\Taxi_track_2024\data\lookups"
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_name=file_path.split("\\")[5].split(".")[0]
            lookups[file_name]=spark.read.csv(file_path,header=True,inferSchema=True)

    return lookups



