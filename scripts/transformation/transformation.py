import io
import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger("airflow.task")
load_dotenv()

# env files
CACHE_PATH = "data/cleaned_data.csv"
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
FILE_PATH_CLEANED = os.getenv("FILE_PATH_CLEANED")
CONTAINER_NAME_SILVER = os.getenv("CONTAINER_NAME_SILVER")

# hook to ads azure
service_client = DataLakeServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    credential=STORAGE_ACCOUNT_KEY
)

# hook & engie sql
pg_hook = PostgresHook(postgres_conn_id='postgre_con')
engine = pg_hook.get_sqlalchemy_engine()


# Main function
def transform():
    origin_df = getData()
    df = origin_df.copy()

    logger.info("[*] Building schema...")
    tables = build_star_scheme(df)
    logger.info("[!] Schema build success")
    
    logger.info("[*] Saving into postgres...")
    save_tables(tables, engine)
    logger.info("[*] Saving complete...")



def build_star_scheme(df):
    # === [Dim table] ===
    dim_products = df[[
        'prod_unique_idx36', 'prod_code', 'prod_name', 'prod_fullname',
        'barcode_ean13', 'prodparent1_code', 'prodparent1_name',
        'prodparent0_code', 'prodparent0_name'
    ]].drop_duplicates(subset=['prod_unique_idx36']).reset_index(drop=True)

    dim_unitmeasure = df[[
        'unitmeasure_code', 'unitmeasure_name'
    ]].drop_duplicates(subset=['unitmeasure_code']).reset_index(drop=True)

    dim_salesmanagers = df[[
        'salesmanager_idx36', 'salesmanager_code', 'salesmanager_name',
        'salesmanager_login_idx36', 'salesmanager_login'
    ]].drop_duplicates(subset=['salesmanager_idx36']).reset_index(drop=True)

    dim_customers = df[[
        'customer_code', 'customer_name', 'discountcardno'
    ]].drop_duplicates(subset=['customer_code']).reset_index(drop=True)

    dim_warehouses = df[[
        'wh_code', 'wh_name'
    ]].drop_duplicates(subset=['wh_code']).reset_index(drop=True)
 
    dim_date = df[[
        'salesdocument_date', 'salesdate_weekday', 'salesdate_weeknum',
        'salesdate_weekstart', 'salesdate_weekend',
        'salesdate_monthnum', 'salesdate_monthdaynum'
    ]].drop_duplicates(subset=['salesdocument_date']).reset_index(drop=True)


    fact_sales = df[[
        # 1. Идентификаторы / дегенеративные измерения
        'salesdocument_unique_idx36', 'salesdocument_number',
        'salesdocument_lineno', 'date_time_iddoc_x36',
 
        # 2. Внешние ключи
        'salesdocument_date',      
        'salesdate_hour',          
        'salesmanager_idx36',      
        'customer_code',           
        'prod_unique_idx36',       
        'wh_code',                 
        'unitmeasure_code',        
 
        # 3. Метрики
        'quantity', 'price', 'totalamount', 'vatamount', 'vat',
        'checksum_totalamount', 'checksum_vatamount',
        'unitmeasure_koef', 'weight', 'volume'
    ]].reset_index(drop=True)

    return {
        'dim_products': dim_products,
        'dim_unitmeasure': dim_unitmeasure,
        'dim_salesmanagers': dim_salesmanagers,
        'dim_customers': dim_customers,
        'dim_warehouses': dim_warehouses,
        'dim_date': dim_date,
        'fact_sales': fact_sales,
    }

# 1. Get data
def getData():
    if os.path.exists(CACHE_PATH):
        print(f'[*] There is: {CACHE_PATH}...')
        df = pd.read_csv(CACHE_PATH, dtype=str)
        return df

    file_system_client = service_client.get_file_system_client(file_system=CONTAINER_NAME_SILVER)
    file_client = file_system_client.get_file_client(FILE_PATH_CLEANED)

    print('[*] Installing file...')
    download = file_client.download_file()
    file_bytes = download.readall()

    df = pd.read_csv(io.BytesIO(file_bytes), dtype=str)

    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    df.to_csv(CACHE_PATH, index=False)
    print(f'[*] Данные сохранены в кэш: {CACHE_PATH}')

    return df

# 2. Save data
def save_tables(tables: dict, engine, if_exists: str = "replace"):

    for name, table_df in tables.items():
        table_df.to_sql(
            name=name,
            con=engine,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=5000,
        )
    print(f"[*] Saved {name} ({len(table_df)} rows)")



if __name__ == '__main__':
    print(pg_hook)