from azure.storage.blob import BlobServiceClient
import argparse
from datetime import datetime, timedelta
import os
import shutil
import pandas as pd
from smbclient import open_file, register_session
import getpass

connect_str = ('DefaultEndpointsProtocol=https;AccountName=eyedropperstorage;AccountKey=GpQKOJHEilzXGgRWd'
               '+X9PAc6oZZxRJNKS3IyiGm9vBMo4Y9S2HQqr6cEAHXrp1YuUUdwAy4r38wy+AStHezi0Q==;EndpointSuffix=core.windows'
               '.net')

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container = "test-data"
download_folder = "./download"
parser = argparse.ArgumentParser(description="Process date range")
parser.add_argument("--start-date", type=str, required=True, help="Start date in YYYY-MM-DD")
parser.add_argument("--end-date", type=str, required=True, help="End date in YYYY-MM-DD")
args = parser.parse_args()
start_date = args.start_date
end_date = args.end_date
combined_file = f"./smart-bottle-{start_date}-{end_date}.csv"


def clear_download_dir():
    if os.path.exists(download_folder):
        shutil.rmtree(download_folder)
    os.makedirs(download_folder)


def download_file():
    container_client = blob_service_client.get_container_client(container)
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

    while start_date_obj <= end_date_obj:
        date_folder = start_date_obj.strftime("output/%Y-%m-%d")
        blobs = container_client.list_blobs(name_starts_with=date_folder)

        for blob in blobs:
            blob_client = container_client.get_blob_client(blob)
            download_path = os.path.join(download_folder, blob.name)

            os.makedirs(os.path.dirname(download_path), exist_ok=True)
            with open(download_path, "wb") as file:
                file.write(blob_client.download_blob().readall())

        start_date_obj += timedelta(days=1)
    print("Download Successfully")


def combine_csv_files():
    all_dataframes = []

    for subdir, dirs, files in os.walk(download_folder):
        dirs.sort()
        for filename in sorted(files):
            if filename.endswith('.csv'):
                file_path = os.path.join(subdir, filename)
                df = pd.read_csv(file_path)
                all_dataframes.append(df)

    combined_df = pd.concat(all_dataframes, ignore_index=True)
    combined_df.to_csv(combined_file, index=False)
    print("Combined Successfully")


def upload_to_turbo():
    hostname = "umms-panewman-win.turbo.storage.umich.edu"
    domain = "umroot"
    username = input("Unique Name: ")
    password = getpass.getpass("Level 1 Password: ")
    share_name = "umms-panewman"
    remote_file_path = (r'\\{host}\{share}\Smart bottle data\UM Smart Bottle Data\Azure IoT\{file}'
                        .format(host=hostname,
                                share=share_name,
                                file=combined_file))
    full_username = f'{domain}\\{username}'
    register_session(hostname, username=full_username, password=password)

    with open(combined_file, 'rb') as src, open_file(remote_file_path, mode='wb') as dst:
        dst.write(src.read())

    print("upload successfully")


clear_download_dir()
download_file()
combine_csv_files()
upload_to_turbo()
