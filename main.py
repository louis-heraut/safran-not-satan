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

# Configuration
CONFIG_FILE = os.getenv("CONFIG_FILE")
config = load_config(CONFIG_FILE)

RESOURCES_DIR = Path("resources")
WELCOME_FILE = RESOURCES_DIR / config['WELCOME_FILE']
METADATA_VARIABLES_FILE = RESOURCES_DIR / config['METADATA_VARIABLES_FILE']
STATE_FILE = config['STATE_FILE']

INDEX_PATH = config['INDEX_PATH']
STAC_CATALOG_PATH = config['STAC_CATALOG_PATH']
STAC_COLLECTION_PATH = config['STAC_COLLECTION_PATH']

DOWNLOAD_DIR = config['DOWNLOAD_DIR']
RAW_DIR = config['RAW_DIR']
SPLIT_DIR = config['SPLIT_DIR']
CONVERT_DIR = config['CONVERT_DIR']
OUTPUT_DIR = config['OUTPUT_DIR']
    
METEO_BASE_URL = config['METEO_BASE_URL']
METEO_DATASET_ID = config['METEO_DATASET_ID']
    
RDG_BASE_URL = config['RDG_BASE_URL']
RDG_DATASET_DOI = config['RDG_DATASET_DOI']
RDG_API_TOKEN = os.getenv("RDG_API_TOKEN")

S3_ENDPOINT = config['S3_ENDPOINT']
S3_BUCKET = config['S3_BUCKET']
S3_PREFIX = config['S3_PREFIX']
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')

S3_REGION = "eu-west-3"


# Setup dev mode
if MODE == "dev":
    try:
        get_ipython().run_line_magic('load_ext', 'autoreload')
        get_ipython().run_line_magic('autoreload', '2')
        print("🔧 Mode développement activé")
        overwrite = True
    except:
        pass


from safran_fairy import download, decompress, split, convert, merge, upload_s3, clean_local, clean_s3, generate_stac_catalog


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
                        help='Upload sur le S3')
    parser.add_argument('--clean', action='store_true',
                        help='Nettoie les anciennes versions')
    parser.add_argument('--ui', action='store_true',
                        help="Update le fichier index de Dataverse")

    # Options
    parser.add_argument('--overwrite',   action='store_true', help='Écrase les fichiers existants sur Dataverse')
    parser.add_argument('--process',     action='store_true', help='Traite uniquement (decompress + split + convert + merge)')

    args = parser.parse_args()
    
    if not any([args.all, args.download, args.decompress, args.split,
                args.convert, args.merge, args.upload, args.clean,
                args.ui]):
        args.all = True
    
    print_welcome(WELCOME_FILE)
    
    # Variables pour tracking
    downloaded_files = None
    decompressed_files = None
    splited_files = None
    converted_files = None
    merged_files = None
    
    # 1. TÉLÉCHARGEMENT
    if args.all or args.download:
        downloaded_files =  download(STATE_FILE, DOWNLOAD_DIR,
                                     METEO_BASE_URL, METEO_DATASET_ID)
        if not downloaded_files:
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
                                  METADATA_VARIABLES_FILE, splited_files)

    # 5. MERGE
    if args.all or args.process or args.merge:
        merged_files = merge(CONVERT_DIR, OUTPUT_DIR, converted_files)

    # 6. UPLOAD
    if args.all or args.upload:
        overwrite = args.overwrite
        not_uploaded = upload_s3(S3_BUCKET=S3_BUCKET,
                                 S3_PREFIX=S3_PREFIX,
                                 OUTPUT_DIR=OUTPUT_DIR,
                                 file_paths=merged_files,
                                 organize_by_version=True,
                                 overwrite=overwrite,
                                 S3_ACCESS_KEY=S3_ACCESS_KEY,
                                 S3_SECRET_KEY=S3_SECRET_KEY,
                                 S3_ENDPOINT=S3_ENDPOINT,
                                 S3_REGION=S3_REGION)
        if not_uploaded:
            sys.exit(1)
            
        
    # 7. NETTOYAGE
    if args.clean:
        # Nettoyage local
        clean_local(directory=DOWNLOAD_DIR)
        clean_local(directory=RAW_DIR)
        clean_local(directory=SPLIT_DIR)
        clean_local(directory=CONVERT_DIR)
        clean_local(directory=OUTPUT_DIR,
                    patterns={'historical': r'historical-(\d{8})-(\d{8})',
                              'latest': r'latest-(\d{8})-(\d{8})',
                              'previous': r'previous-(\d{8})-(\d{8})'})
        
        # Nettoyage S3
        clean_s3(S3_BUCKET=S3_BUCKET,
                 S3_PREFIX=S3_PREFIX, 
                 S3_ACCESS_KEY=S3_ACCESS_KEY,
                 S3_SECRET_KEY=S3_SECRET_KEY,
                 S3_ENDPOINT=S3_ENDPOINT,
                 S3_REGION=S3_REGION)
        

    # 8. UPDATE DATAVERSE
    if args.all or args.ui:
        # generate_index(OUTPUT_DIR=OUTPUT_DIR,
        #                S3_BUCKET=S3_BUCKET,
        #                S3_PREFIX=S3_PREFIX,
        #                METADATA_VARIABLES_FILE=METADATA_VARIABLES_FILE,
        #                INDEX_PATH=INDEX_PATH,
        #                S3_ENDPOINT=S3_ENDPOINT,
        #                S3_REGION=S3_REGION)
        # upload_dataverse_index(INDEX_PATH=INDEX_PATH,
        #                        RDG_DATASET_DOI=RDG_DATASET_DOI,
        #                        RDG_BASE_URL=RDG_BASE_URL,
        #                        RDG_API_TOKEN=RDG_API_TOKEN)


        generate_stac_catalog(S3_BUCKETS3_BUCKET,
                              S3_PREFIX=S3_PREFIX,
                              METADATA_VARIABLES_FILE=METADATA_VARIABLES_FILE,
                              STAC_CATALOG_PATH=STAC_CATALOG_PATH,
                              STAC_COLLECTION_PATH=STAC_COLLECTION_PATH,
                              S3_ACCESS_KEY=S3_ACCESS_KEY,
                              S3_SECRET_KEY=S3_SECRET_KEY,
                              S3_ENDPOINT=S3_ENDPOINT,
                              S3_REGION=S3_REGION)

        upload_s3(S3_BUCKET=S3_BUCKET,
                  S3_PREFIX=S3_PREFIX,
                  OUTPUT_DIR=OUTPUT_DIR,
                  file_paths=[STAC_CATALOG_PATH,
                              STAC_COLLECTION_PATH],
                  overwrite=True)
        
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
