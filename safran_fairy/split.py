import os
from pathlib import Path
import pandas as pd
from art import tprint

from .clean import clean


pd.set_option('display.max_columns', None)
pd.set_option('display.width', 70)
pd.set_option('display.max_colwidth', 50)


def split_file(input_file, SPLIT_DIR):
    print(f"\n✂️ Découpage: {Path(input_file).name}")

    data = pd.read_csv(input_file, sep=";")
    
    SPLIT_DIR = Path(SPLIT_DIR)
    SPLIT_DIR.mkdir(parents=True, exist_ok=True)
    
    base_name = Path(input_file).stem
    id_cols = ['LAMBX', 'LAMBY', 'DATE']
    variables = [col for col in data.columns if col not in id_cols]

    print(f"   → {len(variables)} variables détectées: {', '.join(variables)}")

    splited_files = []
    for var in variables:
        subset = data[id_cols + [var]]
        output_file = SPLIT_DIR / f"{var}_{base_name}.parquet"
        subset.to_parquet(output_file, index=False, compression='snappy')
        splited_files.append(output_file)
        print(f"   💾 {output_file.name}")

    print(f"   ✅ {len(splited_files)} fichiers créés dans {SPLIT_DIR}")
    return splited_files


def split(RAW_DIR, SPLIT_DIR, decompressed_files=None):
    """
    Découpe les fichiers CSV en plusieurs fichiers Parquet, un par variable.

    Orchestre l'appel à split_file() pour chaque fichier CSV du dossier RAW_DIR
    ou de la liste fournie, puis nettoie les fichiers obsolètes dans SPLIT_DIR.

    Args:
        RAW_DIR (str | Path):          Dossier contenant les fichiers CSV décompressés.
        SPLIT_DIR (str | Path):        Dossier de sortie pour les fichiers Parquet.
                                       Créé automatiquement s'il n'existe pas.
        decompressed_files (list[Path], optional): Liste de fichiers CSV à traiter.
                                                   Si None, traite tous les *.csv de RAW_DIR.

    Returns:
        list[list[Path]]: Liste de listes — une sous-liste de fichiers Parquet par CSV traité.
    """

    tprint("split", "small")
    
    Path(SPLIT_DIR).mkdir(parents=True, exist_ok=True)
    
    if decompressed_files is None:
        decompressed_files = list(Path(RAW_DIR).glob("*.csv"))

    print("SPLIT")

    splited_files = []
    for file in decompressed_files:
        output_files = split_file(file, SPLIT_DIR)
        splited_files.append(output_files)

    clean(SPLIT_DIR)
        
    print("\nRÉSUMÉ")
    print(f"   - {len(splited_files)} fichier(s) découpé(s)")
    print(f"   - 📁 Dossier: {os.path.abspath(SPLIT_DIR)}")
        
    return splited_files
