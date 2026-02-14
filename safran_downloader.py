#!/usr/bin/env python3
"""
Script simple pour télécharger les fichiers météo de data.gouv.fr
Télécharge UNIQUEMENT les fichiers qui ont changé
"""

import os
import json
import requests
from datetime import datetime
from pathlib import Path


## CONFIGURATION _______________
CONFIG_FILE = "config.json"

def load_config():
    """Charge la configuration depuis config.json"""
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

config = load_config()

DOWNLOAD_DIR = config['download_dir']
STATE_FILE = config['state_file']
METEO_API_URL = config['meteo_api_url']
METEO_DATASET_ID = config['meteo_dataset_id']

API_URL = METEO_API_URL+DATASET_ID+"/"


## TOOLS _____________
def load_state():
    """Charge l'état des téléchargements précédents"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_state(state):
    """Sauvegarde l'état des téléchargements"""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def get_resources():
    """Récupère la liste des ressources depuis l'API"""
    print("🔍 Récupération de la liste des fichiers...")
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()
    resources = data.get('resources', [])
    print(f"✅ {len(resources)} fichiers trouvés")
    return resources


def has_changed(resource, state):
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


def download_file(resource):
    """Télécharge un fichier"""
    url = resource.get('url')
    if not url:
        print(f"⚠️  Pas d'URL pour {resource.get('title', 'unknown')}")
        return None
    
    # Nom du fichier depuis l'URL
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


def sync():
    """
    Fonction principale de synchronisation
    Télécharge UNIQUEMENT les fichiers qui ont changé
    """
    print("="*60)
    print("🌤️  Synchronisation des données météo")
    print("="*60)
    
    # Créer le dossier de destination
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    # Charger l'état précédent
    state = load_state()
    
    # Récupérer la liste des fichiers
    resources = get_resources()
    
    # Identifier ce qui a changé
    to_download = []
    up_to_date = []
    
    for resource in resources:
        if has_changed(resource, state):
            to_download.append(resource)
        else:
            up_to_date.append(resource)
    
    print(f"\n📊 Analyse:")
    print(f"   - {len(to_download)} fichier(s) à télécharger")
    print(f"   - {len(up_to_date)} fichier(s) déjà à jour")
    
    # Si rien à télécharger
    if not to_download:
        print("\n✨ Tous les fichiers sont à jour!")
        return
    
    # Télécharger ce qui a changé
    print(f"\n{'='*60}")
    print("📥 Téléchargements")
    print(f"{'='*60}")
    
    success = 0
    failed = 0
    
    for i, resource in enumerate(to_download, 1):
        print(f"\n[{i}/{len(to_download)}]")
        result = download_file(resource)
        
        if result:
            # Mettre à jour l'état
            state[resource['id']] = result
            save_state(state)
            success += 1
        else:
            failed += 1
    
    # Résumé
    print(f"\n{'='*60}")
    print("📈 Résumé")
    print(f"{'='*60}")
    print(f"✅ Réussis: {success}")
    print(f"❌ Échecs: {failed}")
    print(f"📁 Dossier: {os.path.abspath(DOWNLOAD_DIR)}")


## RUN ________________
if __name__ == "__main__":
    sync()
