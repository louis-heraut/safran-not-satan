import os
import requests
import json
from pathlib import Path


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
