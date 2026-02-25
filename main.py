#!/usr/bin/env python3
"""
SAFRAN Fairy - Pipeline de traitement des données SAFRAN
"""

import os
import sys
import json
import argparse
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd


def load_config(config_file):
    """Charge la configuration depuis config.json"""
    with open(config_file, 'r') as f:
        return json.load(f)

def print_welcome(welcome_file):
    """Affiche le message de bienvenue"""
    if welcome_file.exists():
        with open(welcome_file, 'r') as f:
            print(f.read())


# Charger env
load_dotenv()
MODE = os.getenv("MODE")

# Setup dev mode
if MODE == "dev":
    try:
        get_ipython().run_line_magic('load_ext', 'autoreload')
        get_ipython().run_line_magic('autoreload', '2')
        print("🔧 Mode développement activé")
    except:
        pass


from safran_fairy import download, decompress, split, convert, merge, upload, clean


def main():
    # Arguments en ligne de commande
    parser = argparse.ArgumentParser(description='SAFRAN Fairy - Pipeline de traitement')

    # Pipeline complet
    parser.add_argument('--all', action='store_true',
                        help='Exécute le pipeline complet')

    # Étapes individuelles
    parser.add_argument('--download', action='store_true',
                        help='Télécharge les fichiers')
    parser.add_argument('--decompress', action='store_true',
                        help='Décompresse les fichiers')
    parser.add_argument('--split', action='store_true',
                        help='Découpe les CSV par variable')
    parser.add_argument('--convert', action='store_true',
                        help='Convertit en NetCDF')
    parser.add_argument('--merge', action='store_true',
                        help='Fusionne temporellement')
    parser.add_argument('--upload', action='store_true',
                        help='Upload sur Dataverse')
    parser.add_argument('--clean', action='store_true',
                        help='Nettoie les anciennes versions')

    # Options
    parser.add_argument('--overwrite',   action='store_true', help='Écrase les fichiers existants sur Dataverse')
    parser.add_argument('--process',     action='store_true', help='Traite uniquement (decompress + split + convert + merge)')

    args = parser.parse_args()
    
    if not any([args.all, args.download, args.decompress, args.split,
                args.convert, args.merge, args.upload, args.clean, args.process]):
        args.all = True

    # Configuration
    CONFIG_FILE = os.getenv("CONFIG_FILE")
    config = load_config(CONFIG_FILE)

    RESOURCES_DIR = Path("resources")
    WELCOME_FILE = RESOURCES_DIR / config['welcome_file']
    METADATA_VARIABLES_FILE = RESOURCES_DIR / config['metadata_variables_file']
    STATE_FILE = config['state_file']
    
    DOWNLOAD_DIR = config['download_dir']
    RAW_DIR = config['raw_dir']
    SPLIT_DIR = config['split_dir']
    CONVERT_DIR = config['convert_dir']
    OUTPUT_DIR = config['output_dir']
    
    METEO_BASE_URL = config['meteo_base_url']
    METEO_DATASET_ID = config['meteo_dataset_id']
    
    RDG_BASE_URL = config['rdg_base_url']
    RDG_DATASET_DOI = config['rdg_dataset_doi']
    RDG_API_TOKEN = os.getenv("RDG_API_TOKEN")
    
    metadata_variables = pd.read_csv(METADATA_VARIABLES_FILE,
                                     index_col='variable')
    
    print_welcome(WELCOME_FILE)
    
    # Variables pour tracking
    downloaded_files = None
    decompressed_files = None
    splited_files = None
    converted_files = None
    merged_files = None
    
    # 1. TÉLÉCHARGEMENT
    if args.all or args.download:
        downloaded_files = download(...)
        if not downloaded_files and args.download:
            print("\n✨ Rien de nouveau à télécharger!")
            if not args.all:
                return

    # 2. DÉCOMPRESSION
    if args.all or args.process or args.decompress:
        decompressed_files = decompress(DOWNLOAD_DIR, RAW_DIR,
                                        downloaded_files)

    # 3. SPLIT
    if args.all or args.process or args.split:
        splited_files = split(RAW_DIR, SPLIT_DIR, decompressed_files)

    # 4. CONVERSION
    if args.all or args.process or args.convert:
        converted_files = convert(SPLIT_DIR, CONVERT_DIR,
                                  metadata_variables, splited_files)

    # 5. MERGE
    if args.all or args.process or args.merge:
        merged_files = merge(CONVERT_DIR, OUTPUT_DIR, converted_files)

    # 6. UPLOAD
    if args.all or args.upload:
        not_uploaded = upload(dataset_DOI=RDG_DATASET_DOI,
                              OUTPUT_DIR=OUTPUT_DIR,
                              file_paths=merged_files,
                              file_categories=file_categories,
                              overwrite=args.overwrite,
                              RDG_BASE_URL=RDG_BASE_URL,
                              RDG_API_TOKEN=RDG_API_TOKEN)
        if not_uploaded:
            sys.exit(1)
        
    # 7. NETTOYAGE
    if args.clean:
        # Nettoyage local
        clean(directory=DOWNLOAD_DIR)
        clean(directory=RAW_DIR)
        clean(directory=SPLIT_DIR)
        clean(directory=CONVERT_DIR)
        clean(directory=OUTPUT_DIR,
              patterns={'historical': r'historical-(\d{8})-(\d{8})',
                        'latest': r'latest-(\d{8})-(\d{8})',
                        'previous': r'previous-(\d{8})-(\d{8})'})
        
        # Nettoyage Dataverse
        clean(dataset_DOI=RDG_DATASET_DOI,
              patterns={'historical': r'historical-(\d{8})-(\d{8})',
                        'latest': r'latest-(\d{8})-(\d{8})',
                        'previous': r'previous-(\d{8})-(\d{8})'},
              RDG_BASE_URL=RDG_BASE_URL,
              RDG_API_TOKEN=RDG_API_TOKEN)
    
    print("\n✨ Pipeline terminé avec succès!")


if __name__ == "__main__":
    try:
        if MODE == "prod":
            main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interruption par l'utilisateur")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
