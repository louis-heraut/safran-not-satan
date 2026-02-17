import os
import requests
import json
import time
from pathlib import Path


def upload_files_to_dataset(dataset_DOI: str,
                            file_paths: list,
                            directory_labels: list = None,
                            RDG_API_URL: str = os.getenv("RDG_API_URL"),
                            RDG_API_TOKEN: str = os.getenv("RDG_API_TOKEN"),
                            verbose: bool = True):
    url = f"{RDG_API_URL}/api/datasets/:persistentId/add?persistentId={dataset_DOI}"
    headers = {'X-Dataverse-key': RDG_API_TOKEN}
    not_uploaded = []

    for i, file_path in enumerate(file_paths):
        path_obj = Path(file_path)
        directory_label = directory_labels[i] if directory_labels else None

        json_data = {"description": "", "restrict": "false", "tabIngest": "true"}
        if directory_label:
            json_data["directoryLabel"] = directory_label

        try:
            start_time = time.time()
            with open(file_path, 'rb') as f:
                files = {
                    'file': (path_obj.name, f),
                    'jsonData': (None, json.dumps(json_data), 'application/json')
                }
                response = requests.post(url, headers=headers, files=files)
            elapsed_time = time.time() - start_time
            file_size = os.path.getsize(file_path) / (1024**2)
            upload_speed = file_size / elapsed_time

            if response.status_code != 200:
                not_uploaded.append(file_path)
                print(f"Failed: {file_path} | {response.status_code} - {response.text}")
            elif verbose:
                dest = f"→ {directory_label}" if directory_label else "→ racine"
                print(f"[{i+1}/{len(file_paths)}] Uploaded {path_obj.name} "
                      f"({round(file_size,2)} MB in {round(elapsed_time,2)}s @ "
                      f"{round(upload_speed,2)} MB/s) {dest}")
        except Exception as e:
            not_uploaded.append(file_path)
            print(f"Error uploading {file_path}: {str(e)}")

    return not_uploaded
