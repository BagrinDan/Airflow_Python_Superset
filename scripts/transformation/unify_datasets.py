import io
import logging
import pandas as pd
from airflow.providers.microsoft.azure.hooks.adls import AzureDataLakeStorageV2Hook

logger = logging.getLogger("airflow.task")

def extract_and_clean_data(azure_conn_id: str, bronze_container: str, silver_container: str):
    """
    Скрипт просто объединяет файлы из Bronze в один датасет
    и сохраняет в Silver для последующего анализа в Jupyter.
    """
    logger.info("Подключение к Azure Data Lake Storage V2...")
    adls_hook = AzureDataLakeStorageV2Hook(adls_conn_id=azure_conn_id)
    
    # Список файлов для объединения
    files_to_read = [
        'Export_Wiz_1_100.csv',
        'Export_Wiz_2_2026_May.csv',
        'Export_Wiz_3_2026_Full.csv'
    ]
    
    dataframes = []
    
    # --- ЭТАП 1: Скачивание ---
    for file_name in files_to_read:
        logger.info(f"Скачивание файла '{file_name}'...")
        try:
            file_bytes = adls_hook.download(file_system_name=bronze_container, file_path=file_name)
            df = pd.read_csv(io.BytesIO(file_bytes))
            dataframes.append(df)
            logger.info(f"Файл {file_name} загружен. Строк: {len(df)}")
        except Exception as e:
            logger.error(f"Ошибка при загрузке {file_name}: {str(e)}")
            raise e

    # --- ЭТАП 2: Объединение (Без очистки) ---
    logger.info("Объединение датасетов воедино...")
    combined_df = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Итоговый сырой датасет содержит: {len(combined_df)} строк.")
    
    # --- ЭТАП 3: Сохранение ---
    output_filename = 'wizard_combined_raw.csv'
    csv_buffer = io.StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    
    logger.info(f"Загрузка объединенного файла '{output_filename}' в контейнер '{silver_container}'...")
    try:
        adls_hook.upload(
            file_system_name=silver_container,
            file_path=output_filename,
            data=csv_buffer.getvalue(),
            overwrite=True
        )
        logger.info("Сборка данных успешно завершена!")
    except Exception as e:
        logger.error(f"Ошибка при записи в Azure: {str(e)}")
        raise e

