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

    for var in variables:
        subset = data[id_cols + [var]]
        output_file = SPLIT_DIR / f"{var}_{base_name}.parquet"
        subset.to_parquet(output_file, index=False, compression='snappy')
    

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
    
    if decompressed_files is not None:
        csv_files = decompressed_files
        print(f"✂️  Découpage de {len(csv_files)} fichier(s) décompressé(s)")
    else:
        csv_files = list(Path(RAW_DIR).glob("*.csv"))
        print(f"✂️  Découpage de TOUS les fichiers ({len(csv_files)})")
    
    split_count = 0

    print(csv_files)
    
    for csv_file in csv_files:
        print(f"\n📄 {csv_file.name}")
        split_file(csv_file, SPLIT_DIR)
        split_count += 1
    
    print(f"\n🎉 {split_count} fichier(s) traité(s)")
    return split_count
