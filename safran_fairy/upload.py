import os
import requests
import json
import time
from pathlib import Path
from art import tprint

from .clean import clean


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


def upload(dataset_DOI: str,
           OUTPUT_DIR: str,
           file_paths: list = None,
           directory_labels: list = None,
           overwrite: bool = False,
           RDG_BASE_URL: str = os.getenv("RDG_BASE_URL"),
           RDG_API_TOKEN: str = os.getenv("RDG_API_TOKEN")):

    tprint("split", "small")
    
    if file_paths is None:
        file_paths = list(Path(OUTPUT_DIR).glob("*.nc"))
    if not file_paths:
        print("\n⚠️  Aucun fichier à uploader")
        return []
    
    print("\nUPLOAD DATAVERSE")
    print(f"   Dataset: {dataset_DOI}")
    print(f"   Fichiers: {len(file_paths)}")

    file_categories = [[f.stem.split('_QUOT_SIM2_')[0],
                        f.stem.split('_QUOT_SIM2_')[1].split('-')[0]]
                       for f in file_paths]
    
    # Récupérer la liste des fichiers existants
    existing_files = get_existing_files(dataset_DOI, RDG_BASE_URL, RDG_API_TOKEN)
    if existing_files:
        print(f"   Fichiers existants: {len(existing_files)}")
    
    url = f"{RDG_BASE_URL}/api/datasets/:persistentId/add?persistentId={dataset_DOI}"
    headers = {'X-Dataverse-key': RDG_API_TOKEN}
    not_uploaded = []
    skipped = []
    
    for i, file_path in enumerate(file_paths):
        path_obj = Path(file_path)
        
        print(f"\n📤 [{i+1}/{len(file_paths)}] {path_obj.name}")
        
        # Vérifier si le fichier existe déjà
        if path_obj.name in existing_files:
            if overwrite:
                # Supprimer l'ancien fichier
                delete_file_by_name(dataset_DOI, path_obj.name, RDG_BASE_URL, RDG_API_TOKEN)
            else:
                # Skip
                print(f"   ⏭️  Fichier déjà présent, ignoré")
                skipped.append(file_path)
                continue
        
        directory_label = directory_labels[i] if directory_labels else None
        categories = file_categories[i] if file_categories else None
        
        if directory_label:
            print(f"   → Dossier: {directory_label}")
        if categories:
            print(f"   🏷️  Catégories: {', '.join(categories)}")
        
        json_data = {"description": "", "restrict": "false", "tabIngest": "true"}
        if directory_label:
            json_data["directoryLabel"] = directory_label
        if categories:
            json_data["categories"] = categories
        
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
            
            if response.status_code not in [200, 201]:
                not_uploaded.append(file_path)
                print(f"   ❌ Échec: {response.status_code} - {response.text}")
            else:
                print(f"   ✅ Upload: {round(file_size, 2)} MB en {round(elapsed_time, 2)}s @ {round(upload_speed, 2)} MB/s")
        
        except Exception as e:
            not_uploaded.append(file_path)
            print(f"   ❌ Erreur: {str(e)}")
    
    print("\nRÉSUMÉ")
    uploaded_count = len(file_paths) - len(not_uploaded) - len(skipped)
    print(f"   - {uploaded_count}/{len(file_paths)} fichier(s) uploadés")
    if skipped:
        print(f"   - ⏭️  {len(skipped)} fichier(s) ignorés (déjà présents)")
    if not_uploaded:
        print(f"   - ⚠️  {len(not_uploaded)} échec(s)")
    
    return not_uploaded



def publish(dataset_DOI: str,
            type: str = "major",
            RDG_BASE_URL: str = os.getenv("RDG_BASE_URL"),
            RDG_API_TOKEN: str = os.getenv("RDG_API_TOKEN")):

    tprint("publish", "small")
    
    print("\nPUBLISH DATASET")
    print(f"   Dataset: {dataset_DOI}")
    print(f"   Type: {type}")
    
    # Construire l'URL
    url = f"{RDG_BASE_URL}/api/datasets/:persistentId/actions/:publish"
    params = {"persistentId": dataset_DOI, "type": type}
    headers = {"X-Dataverse-key": RDG_API_TOKEN}
    
    response = requests.post(url, params=params, headers=headers)
    
    if response.status_code == 200:
        if verbose:
            print(f"\n✅ Dataset {dataset_DOI} publié avec succès")
    else:
        print(f"\n❌ Échec de publication: {response.status_code} - {response.text}")
        return False
    
    return True
