import os
import requests
import json
import time
import pandas as pd
from pathlib import Path
from art import tprint


def get_existing_files(dataset_DOI, RDG_BASE_URL, RDG_API_TOKEN):
    """Récupère la liste des noms de fichiers existants dans le dataset"""
    url = f"{RDG_BASE_URL}/api/datasets/:persistentId/?persistentId={dataset_DOI}"
    headers = {'X-Dataverse-key': RDG_API_TOKEN}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        files_data = response.json()['data']['latestVersion']['files']
        return {file_info['dataFile']['filename'] for file_info in files_data}
    return set()


def delete_file_by_name(dataset_DOI, filename, RDG_BASE_URL, RDG_API_TOKEN):
    """Supprime un fichier par son nom dans le dataset"""
    url = f"{RDG_BASE_URL}/api/datasets/:persistentId/?persistentId={dataset_DOI}"
    headers = {'X-Dataverse-key': RDG_API_TOKEN}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        files_data = response.json()['data']['latestVersion']['files']
        for file_info in files_data:
            if file_info['dataFile']['filename'] == filename:
                file_id = file_info['dataFile']['id']
                delete_url = f"{RDG_BASE_URL}/api/files/{file_id}"
                del_response = requests.delete(delete_url, headers=headers)
                if del_response.status_code in [200, 204]:
                    print(f"   🗑️  Ancien fichier supprimé")
                    return True
    return False


def upload_dataverse(file_path: str,
                     file_description: str = "false",
                     RDG_DATASET_DOI: str,
                     RDG_BASE_URL: str = os.getenv("RDG_BASE_URL"),
                     RDG_API_TOKEN: str = os.getenv("RDG_API_TOKEN")):
    """Upload (ou remplace) un fichier dans un dataset Dataverse."""

    path_obj = Path(file_path)
    headers = {'X-Dataverse-key': RDG_API_TOKEN}

    print(f"\n📤 Upload sur Dataverse: {path_obj.name}")

    # Supprimer l'ancien si présent
    delete_file_by_name(RDG_DATASET_DOI, path_obj.name,
                        RDG_BASE_URL, RDG_API_TOKEN)

    # Upload
    url = f"{RDG_BASE_URL}/api/datasets/:persistentId/add?persistentId={RDG_DATASET_DOI}"
    json_data = {"description": description, 
                 "restrict": "false", 
                 "tabIngest": "false"}

    with open(file_path, 'rb') as f:
        response = requests.post(url, headers=headers, files={
            'file': (path_obj.name, f, 'text/html'),
            'jsonData': (None, json.dumps(json_data), 'application/json')
        })

    if response.status_code in [200, 201]:
        print(f"   ✅ Fichier uploadé")
    else:
        print(f"   ❌ Échec: {response.status_code} - {response.text}")


