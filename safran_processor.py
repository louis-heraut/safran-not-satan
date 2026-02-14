
import json
import gzip
import shutil
from pathlib import Path


## CONFIGURATION _______________
CONFIG_FILE = "config.json"

def load_config():
    """Charge la configuration depuis config.json"""
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

config = load_config()

DOWNLOAD_DIR = config['download_dir']
RAW_DIR = config['raw_dir']


## TOOLS _____
def decompress_file(gz_file, output_dir):
    """Dézippe un fichier .gz dans le dossier output_dir"""
    output_file = Path(output_dir) / gz_file.stem
    with gzip.open(gz_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)  
    return output_file


def decompress_all(download_dir, raw_dir):
    """Dézippe tous les fichiers .gz dans raw_dir"""
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    gz_files = list(Path(download_dir).glob("*.csv.gz"))
    
    for gz_file in gz_files:
        output_file = Path(raw_dir) / gz_file.stem
        decompress_file(gz_file, raw_dir)



decompress_all(DOWNLOAD_DIR, RAW_DIR)
