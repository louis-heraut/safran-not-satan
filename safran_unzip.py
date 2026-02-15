import gzip
import shutil
from pathlib import Path


## TOOLS _____
def decompress_file(gz_file, output_dir):
    """Dézippe un fichier .gz dans le dossier output_dir"""
    output_file = Path(output_dir) / gz_file.stem
    with gzip.open(gz_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)  
    return output_file


def decompress_all(DOWNLOAD_DIR, RAW_DIR, downloaded_files=None):
    """
    Dézippe les fichiers .gz dans raw_dir
    
    Args:
        DOWNLOAD_DIR: Dossier contenant les fichiers téléchargés
        RAW_DIR: Dossier de destination pour les fichiers dézippés
        downloaded_files: Liste optionnelle des fichiers à traiter.
                         Si None, traite TOUS les .csv.gz
    """
    Path(RAW_DIR).mkdir(parents=True, exist_ok=True)
    
    if downloaded_files is not None:
        gz_files = [
            Path(DOWNLOAD_DIR) / filename 
            for filename in downloaded_files 
            if filename.endswith('.csv.gz')
        ]
        print(f"📦 Décompression de {len(gz_files)} fichier(s) téléchargé(s)")
    else:
        gz_files = list(Path(DOWNLOAD_DIR).glob("*.csv.gz"))
        print(f"📦 Décompression de TOUS les fichiers ({len(gz_files)})")
    
    decompressed_files = []
    
    for gz_file in gz_files:
        if not gz_file.exists():
            print(f"⚠️  Fichier introuvable: {gz_file.name}")
            continue
            
        print(f"   → {gz_file.name}")
        output_file = decompress_file(gz_file, RAW_DIR)
        decompressed_files.append(output_file)
    
    print(f"✅ {len(decompressed_files)} fichier(s) décompressé(s)")
    return decompressed_files
