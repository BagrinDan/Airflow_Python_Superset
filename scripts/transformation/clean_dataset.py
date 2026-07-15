import io
import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
import numpy as np
from io import BytesIO

# == GRUPS (DIMENSIONS) ==

# --- IDs & Codes ---
# Технические ключи, штрихкоды и коды для связей
# [DATE_TIME_IDDOC_x36, SalesDocument_Unique_IDx36, SalesDocument_Number, SalesDocument_LineNo]
# [SalesManager_IDx36, SalesManager_Code, SalesManager_Login_IDx36]
# [Customer_Code, DiscountCardNo]
# [Prod_Unique_IDx36, Prod_Code, BarCode_EAN13]
# [ProdParent1_Code, ProdParent0_Code]
# [UnitMeasure_Code, UnitMeasure_Koef, VAT]  

# --- Subject ---
# Люди, контрагенты и сущности
# [SalesManager_Login, SalesManager_Name]
# [Customer_Name]
# [UnitMeasure_Name]

# --- Object ---
# Товарная матрица и иерархия каталога
# [Prod_Name, Prod_FullName]
# [ProdParent1_Name, ProdParent0_Name]

# --- Date ---
# Временные срезы для графиков трендов
# [SalesDocument_Date, SalesDate_Hour, SalesDate_WeekDay, SalesDate_WeekNum]
# [SalesDate_WeekStart, SalesDate_WeekEnd, SalesDate_MonthNum, SalesDate_MonthDayNum]

# --- Location (Где происходило) ---
# [WH_Code, WH_Name]  

# == METRICS (FACTS) ==
# --- Quantity (Объемы) ---
# [Quantity]  

# --- Price & Specs (Цены и физика) ---
# [Price]     <-- Цена за единицу
# [_Weight]   <-- Физический вес (для логистики)
# [_Volume]   <-- Физический объем (для логистики)

# --- Financials (Деньги и контроли) ---
# [TotalAmount, VATamount]  
# [checksum_TotalAmount, checksum_VATAmount] 


CACHE_PATH = "data/raw_data.csv"

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
FILE_PATH = os.getenv("FILE_PATH")

service_client = DataLakeServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    credential=STORAGE_ACCOUNT_KEY
)

def cleaning():
    dataframe = getData();
    df = dataframe.copy()

    # 1. Strip all
    # Strip
    # Выбираем только те колонки, к которым реально применить .str.strip()
    string_cols = df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        df[col] = df[col].astype(str).str.strip()

    # Cleaning DATE_TIME_IDDOC_x36
    sample = '202605017HII4G'
    normal_len = len(sample)
    df["DATE_TIME_IDDOC_x36"] = df["DATE_TIME_IDDOC_x36"].astype(str).str[:normal_len].str.strip()


    # Cleaning BarCode
    df["BarCode_EAN13"] = df["BarCode_EAN13"].replace("", pd.NA)

    mask_invalid = df["BarCode_EAN13"].astype(str).str.len() != 13;
    df.loc[mask_invalid, "BarCode_EAN13"] = pd.NA

    df = df.dropna(subset=["BarCode_EAN13"])


    # Cleaning DiscountCardNo
    df["DiscountCardNo"] = df["DiscountCardNo"].replace('', 'NO_CARD')


    # Cleaning ProdParent0_Code
    df["ProdParent0_Code"] = df["ProdParent0_Code"].replace("Incalzire autonoma si apa", pd.NA)
    df["ProdParent0_Code"] = df["ProdParent0_Code"].replace("+-", pd.NA)
    df['ProdParent0_Code'] = df["ProdParent0_Code"].fillna('Unknown')
    df[df["ProdParent0_Code"].isin(["Incalzire autonoma si apa", "+-"]) & df["ProdParent0_Code"].notna()].shape[0]


    # Cleaning UnitMeasure_Code
    df["UnitMeasure_Code"] = df["UnitMeasure_Code"].replace('Buc', '1')
    mask_numeric = df["UnitMeasure_Code"].astype(str).str.fullmatch(r'\d+')

    cleaned = df.loc[mask_numeric, "UnitMeasure_Code"].astype(str).str.lstrip('0')
    cleaned = cleaned.replace('', '0')

    df.loc[mask_numeric, "UnitMeasure_Code"] = cleaned

    # Cleaning VAT
    df["VAT"] = df["VAT"].astype(str).str.replace('%', '', regex=False)


    # SalesManager_Login ...
    # Customer_Name
    df["Customer_Name"] = df["Customer_Name"].astype(str).str.replace('CARD ', '', regex=False) 
    garbage_pattern = r'\s+(\d{2}\.\d{2}\.\d{4}|\d+%|[A-Za-z]\d+-[A-Za-z\d]+|[A-Za-z]\d+)\s*$'

    df["Customer_Name"] = df["Customer_Name"].astype(str).str.replace(garbage_pattern, '', regex=True)

    # Cleaning UnitMeasure_Name
    unit_mapping = {
        'buc': 'piece',    
        'set': 'pack',     
        'pac': 'pack', 
        'cut': 'pack',
        'sac': 'weight',   
        'kg': 'weight',
        'm': 'length',     
        'm2': 'area',      
        'rul': 'length',   
        'per': 'piece',    
        'unknown': 'unknown'
    }

    df["UnitMeasure_Name"] = df["UnitMeasure_Name"].fillna("unknown").astype(str).str.lower()
    df["UnitMeasure_Name"] = df["UnitMeasure_Name"].replace(unit_mapping)

    # Prod_Name
    df['Prod_Name']= df['Prod_Name'].astype(str).str.lower()
    df['Prod_Name'] = df['Prod_Name'].str.replace(r'\s*\([^)]*\)', '', regex=True)
    df['Prod_Name'] = df['Prod_Name'].str.replace(r'\s+([a-z]*\d{4,7}|\d{4,7})\b\s*$', '', regex=True)
    df['Prod_Name'] = df['Prod_Name'].str.strip()

    # ProdParent1_Name
    df['ProdParent1_Name'] = (df['ProdParent1_Name'].astype(str)
                            .str.lower()
                            .str.replace(r'[\s]*,[\s]*', ' / ', regex=True)
                            .str.replace(r'\s+', ' ', regex=True)
                            .str.strip())
                            

    # ProdParent0_Name
    df['ProdParent0_Name'] = (df['ProdParent0_Name'].fillna('unknown').astype(str)
                            .str.lower())      

    # SalesDate_WeekDay
    week_day={
        '1':'Monday',
        '2': 'Tuesday',
        '3': 'Wednesday',
        '4': 'Thursday',
        '5': 'Friday',
        '6': 'Saturday',
        '7': 'Sunday'
    }

    df['SalesDate_WeekDay'] = (df['SalesDate_WeekDay'].astype(str).replace(week_day)
                            .replace('0', 'unknown_day'))

    # SalesDate_MonthNum
    month_map = {
        '1': 'January',
        '2': 'February',
        '3': 'March',
        '4': 'April',
        '5': 'May',
        '6': 'June',
        '7': 'July',
        '8': 'August',
        '9': 'September',
        '10': 'October',
        '11': 'November',
        '12': 'December'
    }
    df['SalesDate_MonthNum'] = (df['SalesDate_MonthNum'].astype(str).replace(month_map)
                                .replace('0', 'Unknown_month'))

    # WH_Code
    df['WH_Name'] = df['WH_Name'].astype(str).str.replace(r'^_+', '', regex=True).str.strip()
    is_numeric_string = df['WH_Name'].str.contains(r'^\d+\.\d+$|^\d+$', regex=True, na=False)
    df['WH_Name'] = np.where(is_numeric_string, 'Unknown_Warehouse', df['WH_Name'])
    df['WH_Name'] = df['WH_Name'].replace(['nan', 'None'], 'Unknown_Warehouse').fillna('Unknown_Warehouse')

    # Quantity
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0.0)

    date_cols = ['SalesDocument_Date', 'SalesDate_WeekStart', 'SalesDate_WeekEnd']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # 2. Переводим Дробные числа (float)
    float_cols = [
        'Quantity', 'Price', 'TotalAmount', 'VATamount', 'VAT', 
        'checksum_TotalAmount', 'checksum_VATAmount', 'UnitMeasure_Koef', '_Weight', '_Volume'
    ]
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    # 3. Переводим Целые числа (int)
    # Убрал 'SalesDate_MonthNum' из int_cols, так как выше мы перевели её в текстовые названия месяцев ('January' и т.д.)
    int_cols = ['SalesDocument_LineNo', 'SalesDate_Hour', 'SalesDate_WeekNum', 'SalesDate_MonthDayNum']
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # 4. Переводим повторяющийся текст в Категории (для оптимизации памяти)
    # Добавил 'SalesDate_MonthNum' сюда, так как теперь это текстовые категории месяцев
    cat_cols = ['SalesDate_WeekDay', 'SalesDate_MonthNum', 'SalesManager_Name', 'WH_Name', 'ProdParent0_Name', 'ProdParent1_Name', 'UnitMeasure_Name']
    for col in cat_cols:
        df[col] = df[col].astype('category')

    # Lowercase columns and strip
    df.columns = df.columns.str.lower().str.strip().str.lstrip('_')

    df.info()

    save_data(df, service_client)


# 1. Get data
def getData():
    if os.path.exists(CACHE_PATH):
        print(f'[*] There is: {CACHE_PATH}...')
        df = pd.read_csv(CACHE_PATH, dtype=str)
        return df

    file_system_client = service_client.get_file_system_client(file_system=CONTAINER_NAME)
    file_client = file_system_client.get_file_client(FILE_PATH)

    print('[*] Installing file...')
    download = file_client.download_file()
    file_bytes = download.readall()

    df = pd.read_csv(io.BytesIO(file_bytes), dtype=str)

    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    df.to_csv(CACHE_PATH, index=False)
    print(f'[*] Данные сохранены в кэш: {CACHE_PATH}')

    return df


# Save data
def save_data(df, service_client):
    silver_fs_client = service_client.get_file_system_client(file_system="silver")
    silver_file_client = silver_fs_client.get_file_client("clean_dataset.csv")

    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    data = buffer.getvalue()

    silver_file_client.upload_data(data, overwrite=True)

    print(f"Saved cleaned dataset to silver/clean_dataset.csv ({len(df)} rows)")


if __name__ == '__main__':
    clean_df = cleaning()

    
