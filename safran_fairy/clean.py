from pathlib import Path
import re
from datetime import datetime
from art import tprint


def clean(directory,
          extensions=['.csv', '.csv.gz', '.parquet', '.nc'],
          patterns = {
            'latest': r'latest-(\d{8})-(\d{8})',
            'previous': r'previous-(\d{4})-(\d{6})'
          }):
    """
    Supprime les fichiers obsolètes d'un dossier en ne gardant que le plus récent par type.

    Pour chaque type défini dans patterns, identifie les fichiers correspondants
    et supprime ceux dont la date (2ème groupe capturant) est inférieure au maximum.

    Args:
        directory (str | Path): Dossier à nettoyer.
        extensions (list[str]): Extensions de fichiers à considérer.
                                Défaut: ['.csv', '.csv.gz', '.parquet', '.nc']
        patterns (dict[str, str]): Patterns regex par type de fichier.
                                   La date de comparaison est extraite du 2ème groupe capturant.
                                   Défaut: latest et previous.

    Returns:
        None
    """
        
    directory = Path(directory)

    print("\nNETTOYAGE")
    
    for file_type, pattern in patterns.items():
        print(f"\nRecherche de fichiers '{file_type}'...")
        
        files = list(directory.glob(f"*{file_type}*"))
        files = [f for f in files if ''.join(f.suffixes) in extensions]
        files = [f for f in files if re.search(pattern, f.name)]
        
        if not files:
            print(f"   - ℹ️ Aucun fichier trouvé")
            continue
        
        dates = [int(re.search(pattern, f.name).group(2)) for f in files]
        max_date = max(dates)
        files_to_delete = [f for f, d in zip(files, dates) if d < max_date]

        for file in files_to_delete:
            print(f"   - 🗑️ {file.name}")
            file.unlink()
        
        print(f"   - 📊 {len(files_to_delete)} fichier(s) supprimé(s)")

        
