#!/usr/bin/env python3
"""
Script principal : télécharge, dézippe, traite
"""

get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')


import json
from pathlib import Path

from safran_downloader import sync
from safran_clean import clean_files
from safran_unzip import decompress_all
from safran_split import split_all
from safran_convert import convert_files
from safran_combine import merge_files


## CONFIGURATION _______________
CONFIG_FILE = "config.json"

def load_config(CONFIG_FILE):
    """Charge la configuration depuis config.json"""
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

config = load_config(CONFIG_FILE)

STATE_FILE = config['state_file']
DOWNLOAD_DIR = config['download_dir']
RAW_DIR = config['raw_dir']
SPLIT_DIR = config['split_dir']
CONVERT_DIR = config['convert_dir']
OUTPUT_DIR = config['output_dir']

METEO_API_URL = config['meteo_api_url']
METEO_DATASET_ID = config['meteo_dataset_id']
API_URL = METEO_API_URL + METEO_DATASET_ID + "/"


## RUN _____________
def main():
    """Pipeline complet"""
    
    # 1. Téléchargement
    downloaded_files = sync(API_URL, STATE_FILE, DOWNLOAD_DIR)
    
    if not downloaded_files:
        print("\n✨ Rien de nouveau à traiter!")
        return

    clean_files(DOWNLOAD_DIR)
    
    # 2. Dézipage
    decompressed_files = decompress_all(DOWNLOAD_DIR, RAW_DIR,
                                        downloaded_files)
    clean_files(RAW_DIR)
        
    # 3. Traitement
    splited_files = split_all(RAW_DIR, SPLIT_DIR, decompressed_files)
    clean_files(SPLIT_DIR)

    # 4. Conversion NetCDF
    ### tmp
    splited_files = Path(SPLIT_DIR).glob("*.parquet")
    splited_files = [f for f in splited_files if "TINF_H_QUOT" in f.name]
    ###

    converted_files = convert_files(SPLIT_DIR, CONVERT_DIR, splited_files)


    # 5. Concaténer
    merge_files(SPLIT_DIR, OUTPUT_DIR, converted_files)
        






        
# if __name__ == "__main__":
# main()
