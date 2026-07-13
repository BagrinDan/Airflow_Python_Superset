import io
import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
import numpy as np
from io import BytesIO

CACHE_PATH = "data/cleaned_data.csv"


STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
FILE_PATH_CLEANED = os.getenv("FILE_PATH_CLEANED")

service_client = DataLakeServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    credential=STORAGE_ACCOUNT_KEY
)


def transform():
    pass

# 1. Get data
def getData():
    if os.path.exists(CACHE_PATH):
        print(f'[*] There is: {CACHE_PATH}...')
        df = pd.read_csv(CACHE_PATH, dtype=str)
        return df

    file_system_client = service_client.get_file_system_client(file_system=CONTAINER_NAME)
    file_client = file_system_client.get_file_client(FILE_PATH_CLEANED)

    print('[*] Installing file...')
    download = file_client.download_file()
    file_bytes = download.readall()

    df = pd.read_csv(io.BytesIO(file_bytes), dtype=str)

    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    df.to_csv(CACHE_PATH, index=False)
    print(f'[*] Данные сохранены в кэш: {CACHE_PATH}')

    return df


def save_data(df, service_client):
    silver_fs_client = service_client.get_file_system_client(file_system="silver")
    silver_file_client = silver_fs_client.get_file_client("transformed.csv")

    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    data = buffer.getvalue()

    silver_file_client.upload_data(data, overwrite=True)

    print(f"[*] Saved cleaned dataset to silver/transformed.csv ({len(df)} rows)")



if __name__ == '__main__':
    pass