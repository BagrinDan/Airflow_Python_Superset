import io
import logging
import pandas as pd
from typing import List
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from ..helper import manifest

logger = logging.getLogger("airflow.task")

def _get_new_files(adls_hook: AzureDataLakeStorageV2Hook, source_container: str) -> List[str]:
    """Проверяет манифест и возвращает список только новых CSV файлов."""
    logger.info("[*] Проверка входящих датасетов...")
    processed_files = manifest.get_processed_files(adls_hook, source_container)
    
    paths = adls_hook.list_files_directory(source_container, 'datasets')
    all_files = [p.split('/')[-1] for p in paths if p.endswith('.csv')]
    
    return [f for f in all_files if f not in processed_files]


def _download_bronze_files(adls_hook: AzureDataLakeStorageV2Hook, container: str, file_names: List[str]) -> List[pd.DataFrame]:

    dataframes = []
    file_system_client = adls_hook.get_conn().get_file_system_client(container)
    
    for file_name in file_names:
        full_source_path = f"datasets/{file_name}"
        logger.info(f"Скачивание: {container}/{full_source_path}")
        try:
            file_client = file_system_client.get_file_client(full_source_path)
            file_bytes = file_client.download_file().readall()
            df = pd.read_csv(io.BytesIO(file_bytes), encoding='windows-1251')

            dataframes.append(df)
            logger.info(f"Успешно загружен {file_name}. Строк: {len(df)}")
        except Exception as e:
            logger.error(f"Ошибка при загрузке {full_source_path}: {str(e)}")
            raise e
    return dataframes


def _download_existing_unified(adls_hook: AzureDataLakeStorageV2Hook, container: str, path: str) -> pd.DataFrame:

    target_fs_client = adls_hook.get_conn().get_file_system_client(container)
    try:
        existing_file_client = target_fs_client.get_file_client(path)
        existing_bytes = existing_file_client.download_file().readall()
        logger.info("Подтянуты существующие исторические данные.")
        return pd.read_csv(io.BytesIO(existing_bytes))
    except Exception:
        logger.info("Существующего unified-файла ещё нет, будет создан с нуля.")
        return pd.DataFrame()


def unify_dataset(azure_conn_id: str, source_container: str, target_container: str):
    """Основной метод-оркестратор."""
    logger.info("[*] Подключение к Azure Data Lake...")
    adls_hook = AzureDataLakeStorageV2Hook(adls_conn_id=azure_conn_id)

    # 1. Получаем список новых файлов
    new_files = _get_new_files(adls_hook, source_container)
    if not new_files:
        logger.info("Новых файлов нет, пропускаем объединение")
        return
    logger.info(f"Найдено новых файлов: {len(new_files)}")

    # 2. Скачиваем новые данные и историю
    dataframes = _download_bronze_files(adls_hook, source_container, new_files)
    
    full_target_path = "all_in_one/wizard_combined_raw.csv"
    existing_df = _download_existing_unified(adls_hook, target_container, full_target_path)
    
    if not existing_df.empty:
        dataframes.append(existing_df)

    # 3. Объединяем
    logger.info("Объединение датасетов...")
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # 4. Сохраняем результат
    csv_buffer = io.StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    
    logger.info(f"Загрузка объединенного файла в: {target_container}/{full_target_path}")
    try:
        target_fs_client = adls_hook.get_conn().get_file_system_client(target_container)
        
        file_client = target_fs_client.get_file_client(full_target_path)

        file_client.upload_data(combined_df.to_csv(index=False), overwrite=True)
        
        logger.info("Объединение и запись в подпапку успешно завершены!")
    except Exception as e:
        logger.error(f"Ошибка при записи файла {full_target_path}: {str(e)}")
        raise e