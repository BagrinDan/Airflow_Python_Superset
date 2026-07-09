import json

def get_processed_files(adls_hook, container, manifest_path='datasets/_processed_manifest.json'):
    try:
        file_system_client = adls_hook.get_conn().get_file_system_client(container)
        file_client = file_system_client.get_file_client(manifest_path)
        content = file_client.download_file().readall()
        return set(json.loads(content))
    except Exception:
        return set()

def update_manifest(adls_hook, container, processed_files, manifest_path='datasets/_processed_manifest.json'):
    file_system_client = adls_hook.get_conn().get_file_system_client(container)
    file_client = file_system_client.get_file_client(manifest_path)
    content = json.dumps(list(processed_files)).encode('utf-8')
    file_client.upload_data(content, overwrite=True)