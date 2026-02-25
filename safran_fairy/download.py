import json
import os
import requests
from datetime import datetime
from pathlib import Path
from art import tprint

from .clean import clean


def load_state(STATE_FILE):
    """Charge l'état des téléchargements précédents"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_state(state, STATE_FILE):
    """Sauvegarde l'état des téléchargements"""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def get_resources(API_URL):
    """Récupère la liste des ressources depuis l'API"""
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()
    resources = data.get('resources', [])
    return resources


def has_changed(resource, state, DOWNLOAD_DIR):
    """
    Vérifie si un fichier a changé depuis le dernier téléchargement
    Compare la date 'last_modified' de l'API avec celle sauvegardée
    """
    resource_id = resource['id']
    
    # Si jamais téléchargé → oui, il a "changé"
    if resource_id not in state:
        return True
    
    # Comparer la date de modification
    current_date = resource.get('last_modified')
    saved_date = state[resource_id].get('last_modified')
    
    if current_date != saved_date:
        return True
    
    # Vérifier si le fichier existe encore localement
    filename = state[resource_id].get('filename')
    if filename and not os.path.exists(os.path.join(DOWNLOAD_DIR, filename)):
        return True
    
    return False


def download_file(resource, DOWNLOAD_DIR):
    """Télécharge un fichier"""
    url = resource.get('url')
    
    filename = url.split('/')[-1].split('?')[0]
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    
    print(f"\n📥 Téléchargement: {resource.get('title', filename)}")
    print(f"   → {filepath}")
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        print(f"   Progression: {percent:.1f}%", end='\r')
        
        size_mb = downloaded / (1024*1024)
        print(f"\n   ✅ Téléchargé: {size_mb:.2f} Mo")
        
        return {
            'filename': filename,
            'last_modified': resource.get('last_modified'),
            'downloaded_at': datetime.now().isoformat(),
            'size_bytes': downloaded
        }
        
    except Exception as e:
        print(f"   ❌ Erreur: {e}")
        return None


def download(STATE_FILE, DOWNLOAD_DIR, METEO_BASE_URL, METEO_DATASET_ID):
    """
    Synchronise les fichiers depuis l'API en téléchargeant uniquement ceux qui ont changé.

    Compare la date 'last_modified' de chaque ressource avec l'état sauvegardé,
    télécharge les fichiers nouveaux ou modifiés, puis met à jour l'état.

    Args:
        BASE_URL (str):      URL de l'API retournant la liste des ressources.
        STATE_FILE (str):   Chemin du fichier JSON de suivi des téléchargements.
                            Créé automatiquement au premier appel.
        DOWNLOAD_DIR (str): Dossier de destination pour les fichiers téléchargés.
                            Créé automatiquement s'il n'existe pas.

    Returns:
        list[str] | None: Noms des fichiers téléchargés avec succès,
                          ou None si tout est déjà à jour.
                          Ex: ['QUOT_SIM2_1958-1959.csv.gz', ...]

    Notes:
        - L'état est sauvegardé après chaque téléchargement réussi.
    """
  
    tprint("download", "small")

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    state = load_state(STATE_FILE)    
    API_URL = METEO_BASE_URL + METEO_DATASET_ID + "/"
    resources = get_resources(API_URL)
    
    to_download = []
    up_to_date = []
    
    for resource in resources:
        if has_changed(resource, state, DOWNLOAD_DIR):
            to_download.append(resource)
        else:
            up_to_date.append(resource)
    
    print("ANALYSE")
    print(f"\n   - {len(to_download)} fichier(s) à télécharger")
    print(f"   - {len(up_to_date)} fichier(s) déjà à jour")
    
    if not to_download:
        print("\n✨ Tous les fichiers sont à jour!")
        return

    print("\nTÉLÉCHARGEMENT")
    
    success = 0
    failed = 0
    downloaded_files = []
    
    for i, resource in enumerate(to_download, 1):
        print(f"\n[{i}/{len(to_download)}]")
        result = download_file(resource, DOWNLOAD_DIR)
        
        if result:
            state[resource['id']] = result
            save_state(state, STATE_FILE)
            success += 1
            downloaded_files.append(result['filename'])
        else:
            failed += 1

    clean(DOWNLOAD_DIR)
            
    print("\nRÉSUMÉ")
    print(f"   - ✅ Réussis: {success}")
    print(f"   - ❌ Échecs: {failed}")
    print(f"   - 📁 Dossier: {os.path.abspath(DOWNLOAD_DIR)}")

    downloaded_files = [Path(f) for f in downloaded_files if f.endswith('.csv.gz')]
    return downloaded_files

