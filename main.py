#!/usr/bin/env python3
"""
Script principal : télécharge, dézippe, traite
"""

get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')

import os
import json
from pathlib import Path
from dotenv import load_dotenv
from art import tprint

from safran_fairy import download, decompress, split, convert, merge


## CONFIGURATION _______________
CONFIG_FILE = "config.json"

def load_config(CONFIG_FILE):
    """Charge la configuration depuis config.json"""
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

config = load_config(CONFIG_FILE)
load_dotenv()

WELCOME_FILE = config['welcome_file']
STATE_FILE = config['state_file']

DOWNLOAD_DIR = config['download_dir']
RAW_DIR = config['raw_dir']
SPLIT_DIR = config['split_dir']
CONVERT_DIR = config['convert_dir']
OUTPUT_DIR = config['output_dir']

METEO_API_URL = config['meteo_api_url']
METEO_DATASET_ID = config['meteo_dataset_id']

RDG_API_URL = config['rdg_api_url']
RDG_DATASET_DOI = config['rdg_dataset_doi']
RDG_API_TOKEN = os.getenv("RDG_API_TOKEN")


## RUN _____________
def main():
    with open(WELCOME_FILE, 'r') as f:
        print(f.read())
    
    # 1. Téléchargement
    downloaded_files = download(STATE_FILE, DOWNLOAD_DIR,
                                METEO_API_URL, METEO_DATASET_ID)
    
    if not downloaded_files:
        print("\n✨ Rien de nouveau à traiter!")
        return
    
    # 2. Dézipage
    decompressed_files = decompress(DOWNLOAD_DIR, RAW_DIR,
                                    downloaded_files)
        
    # 3. Traitement
    splited_files = split(RAW_DIR, SPLIT_DIR, decompressed_files)

    # 4. Conversion NetCDF
    converted_files = convert(SPLIT_DIR, CONVERT_DIR, splited_files)

    # 5. Concaténer
    merged_files = merge(CONVERT_DIR, OUTPUT_DIR, converted_files)

    # 6. Upload
    upload_files_to_dataset(dataset_DOI=RDG_DATASET_DOI,
                            file_paths=merged_files,
                            RDG_API_URL=RDG_API_URL,
                            RDG_API_TOKEN=RDG_API_TOKEN)

        
# if __name__ == "__main__":
# main()

