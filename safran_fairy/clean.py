import os
import re
import requests
from pathlib import Path
from datetime import datetime
from art import tprint
import boto3


def clean_local(directory, extensions, patterns):
    """Version locale de ton code actuel"""
    directory = Path(directory)
    print("\nNETTOYAGE")
    
    for file_type, pattern in patterns.items():
        print(f"\nRecherche de fichiers '{file_type}'...")
        
        files = list(directory.glob(f"*{file_type}*"))
        files = [f for f in files if ''.join(f.suffixes) in extensions]
        files = [f for f in files if re.search(pattern, f.name)]
        
        if not files:
            print(f"   - ℹ️ Aucun fichier trouvé")
            continue
        
        dates = [int(re.search(pattern, f.name).group(2)) for f in files]
        max_date = max(dates)
        files_to_delete = [f for f, d in zip(files, dates) if d < max_date]
        for file in files_to_delete:
            print(f"   - 🗑️ {file.name}")
            file.unlink()
        
        print(f"   - 📊 {len(files_to_delete)} fichier(s) supprimé(s)")


def clean_dataverse(dataset_DOI: str,
                    RDG_BASE_URL: str = os.getenv("RDG_BASE_URL"),
                    RDG_API_TOKEN: str = os.getenv("RDG_API_TOKEN"),
                    extensions=['.csv', '.csv.gz', '.parquet', '.nc'],
                    patterns = {
                        'latest': r'latest-(\d{8})-(\d{8})',
                        'previous': r'previous-(\d{4})-(\d{6})'
                    }):
    """
    Supprime les fichiers obsolètes d'un dataset Dataverse en ne gardant que le plus récent par type.
    """
    
    print("\nNETTOYAGE")
    print(f"   Dataset: {dataset_DOI}")
    
    # Récupérer la liste des fichiers du dataset
    url = f"{RDG_BASE_URL}/api/datasets/:persistentId/?persistentId={dataset_DOI}"
    headers = {'X-Dataverse-key': RDG_API_TOKEN}
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"   ❌ Impossible de récupérer les fichiers: {response.text}")
        return
    
    files_data = response.json()['data']['latestVersion']['files']
    
    for file_type, pattern in patterns.items():
        print(f"\nRecherche de fichiers '{file_type}'...")
        
        # Filtrer les fichiers qui matchent le pattern ET l'extension
        matching_files = []
        for file_info in files_data:
            filename = file_info['dataFile']['filename']
            
            # Vérifier l'extension
            file_ext = ''.join(Path(filename).suffixes)
            if file_ext not in extensions:
                continue
            
            # Vérifier le pattern
            if re.search(pattern, filename):
                date = int(re.search(pattern, filename).group(2))
                matching_files.append({
                    'id': file_info['dataFile']['id'],
                    'filename': filename,
                    'date': date
                })
        
        if not matching_files:
            print(f"   - ℹ️ Aucun fichier trouvé")
            continue
        
        # Identifier les fichiers à supprimer (tous sauf le plus récent)
        max_date = max(f['date'] for f in matching_files)
        files_to_delete = [f for f in matching_files if f['date'] < max_date]
        
        # Supprimer les fichiers obsolètes
        deleted_count = 0
        for file in files_to_delete:
            delete_url = f"{RDG_BASE_URL}/api/files/{file['id']}"
            del_response = requests.delete(delete_url, headers=headers)
            if del_response.status_code in [200, 204]:  # <-- ajout du 200
                print(f"   - 🗑️ {file['filename']}")
                deleted_count += 1
            else:
                print(f"   - ❌ Échec suppression {file['filename']}: {del_response.text}")

        print(f"   - 📊 {deleted_count}/{len(files_to_delete)} fichier(s) supprimé(s)")


def clean_s3(S3_BUCKET: str,
             S3_PREFIX: str = "",
             extensions=['.csv', '.csv.gz', '.parquet', '.nc'],
             patterns={
                 'latest': r'latest-(\d{8})-(\d{8})',
                 'previous': r'previous-(\d{4})-(\d{6})'
             },
             S3_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY"),
             S3_SECRET_KEY: str = os.getenv("S3_SECRET_KEY"),
             S3_ENDPOINT: str = os.getenv("S3_ENDPOINT"),
             S3_REGION: str = os.getenv("S3_REGION", "eu-west-1")):
    """
    Supprime les fichiers obsolètes d'un bucket S3 en ne gardant que le plus récent par type.
    """

    s3 = boto3.client('s3',
                      aws_access_key_id=S3_ACCESS_KEY,
                      aws_secret_access_key=S3_SECRET_KEY,
                      endpoint_url=S3_ENDPOINT,
                      region_name=S3_REGION)

    print("\nNETTOYAGE S3")
    print(f"   Bucket: {S3_BUCKET}/{S3_PREFIX or ''}")

    all_objects = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get('Contents', []):
            all_objects.append(obj['Key'])
    
    for file_type, pattern in patterns.items():
        print(f"\nRecherche de fichiers '{file_type}'...")
        
        matching_files = []
        for key in all_objects:
            filename = Path(key).name
            
            # Vérifier l'extension
            file_ext = ''.join(Path(filename).suffixes)
            if file_ext not in extensions:
                continue
            
            # Vérifier le pattern
            match = re.search(pattern, filename)
            if match:
                date = int(match.group(2))
                matching_files.append({'key': key, 'filename': filename, 'date': date})
        
        if not matching_files:
            print(f"   - ℹ️ Aucun fichier trouvé")
            continue
        
        max_date = max(f['date'] for f in matching_files)
        files_to_delete = [f for f in matching_files if f['date'] < max_date]
        
        if not files_to_delete:
            print(f"   - ✅ Déjà propre (fichier le plus récent: {max_date})")
            continue
        
        deleted_count = 0
        for file in files_to_delete:
            try:
                s3.delete_object(Bucket=S3_BUCKET, Key=file['key'])
                print(f"   - 🗑️ {file['filename']}")
                deleted_count += 1
            except Exception as e:
                print(f"   - ❌ Échec suppression {file['filename']}: {str(e)}")
        
        print(f"   - 📊 {deleted_count}/{len(files_to_delete)} fichier(s) supprimé(s)")


def clean(directory=None,
          dataset_DOI=None,
          S3_BUCKET=None,
          S3_PREFIX="",
          extensions=['.csv', '.csv.gz', '.parquet', '.nc'],
          patterns={
              'latest': r'latest-(\d{8})-(\d{8})',
              'previous': r'previous-(\d{4})-(\d{6})'
          },
          RDG_BASE_URL: str = os.getenv("RDG_BASE_URL"),
          RDG_API_TOKEN: str = os.getenv("RDG_API_TOKEN"),
          S3_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY"),
          S3_SECRET_KEY: str = os.getenv("S3_SECRET_KEY"),
          S3_ENDPOINT: str = os.getenv("S3_ENDPOINT"),
          S3_REGION: str = os.getenv("S3_REGION", "eu-west-1")):
    """
    Nettoie un dossier local ET/OU un dataset Dataverse ET/OU un bucket S3.
    """
    
    if directory:
        clean_local(directory=directory, extensions=extensions, patterns=patterns)
    
    if dataset_DOI:
        clean_dataverse(dataset_DOI=dataset_DOI, extensions=extensions, patterns=patterns,
                        RDG_BASE_URL=RDG_BASE_URL, RDG_API_TOKEN=RDG_API_TOKEN)
    
    if S3_BUCKET:
        clean_s3(S3_BUCKET=S3_BUCKET,
                 S3_PREFIX=S3_PREFIX,
                 extensions=extensions,
                 patterns=patterns,
                 S3_ACCESS_KEY=S3_ACCESS_KEY,
                 S3_SECRET_KEY=S3_SECRET_KEY,
                 S3_ENDPOINT=S3_ENDPOINT,
                 S3_REGION=S3_REGION)
