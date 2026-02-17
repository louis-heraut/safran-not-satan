import os
from pathlib import Path
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 70)
pd.set_option('display.max_colwidth', 50)


def split_file(input_file, SPLIT_DIR):
    """
    Découpe un fichier CSV en plusieurs fichiers parquet, un par variable
    
    Args:
        input_file: chemin du fichier CSV source (ex: QUOT_SIM2_1958-1959.csv)
        SPLIT_DIR: dossier de sortie pour les fichiers parquet
    """
    # Lire le fichier
    data = pd.read_csv(input_file, sep=";")
    
    # Créer le dossier de sortie
    SPLIT_DIR = Path(SPLIT_DIR)
    SPLIT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Extraire le nom de base (QUOT_SIM2_1958-1959)
    base_name = Path(input_file).stem
    
    # Colonnes qui sont des identifiants (à garder dans tous les fichiers)
    id_cols = ['LAMBX', 'LAMBY', 'DATE']
    
    # Variables à extraire (toutes sauf les ID)
    variables = [col for col in data.columns if col not in id_cols]

    splited_files = []
    for var in variables:
        subset = data[id_cols + [var]]
        output_file = SPLIT_DIR / f"{var}_{base_name}.parquet"
        subset.to_parquet(output_file, index=False, compression='snappy')
        splited_files.append(output_file)

    return splited_files
    

def split_all(RAW_DIR, SPLIT_DIR, decompressed_files=None):
    """
    Découpe les fichiers CSV en plusieurs fichiers parquet, un par variable
    
    Args:
        RAW_DIR: Dossier contenant les fichiers CSV décompressés
        SPLIT_DIR: Dossier de sortie pour les fichiers parquet
        decompressed_files: Liste optionnelle des fichiers à traiter.
                           Si None, traite TOUS les .csv
    """
    Path(SPLIT_DIR).mkdir(parents=True, exist_ok=True)
    
    if decompressed_files is None:
        decompressed_files = list(Path(RAW_DIR).glob("*.csv"))
    
    splited_files = []
    for file in decompressed_files:
        output_files = split_file(file, SPLIT_DIR)
        splited_files.append(output_files)

    return splited_files
