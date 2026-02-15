from pathlib import Path
import re
from datetime import datetime


def clean_files(directory):
    """
    Supprime les anciens fichiers latest et previous, garde seulement les plus récents
    
    Args:
        directory: Dossier à nettoyer
    """
    patterns = {
        'latest': r'latest-(\d{8})-(\d{8})',
        'previous': r'previous-(\d{4})-(\d{6})'
    }
    
    extensions = ['.csv', '.csv.gz', '.parquet']
    
    directory = Path(directory)
    
    for file_type, pattern in patterns.items():
        print(f"\n🔍 Nettoyage '{file_type}'...")
                
        files = directory.glob(f"*{file_type}*")
        files = [f for f in files if ''.join(f.suffixes) in extensions]
        files = [f for f in files if re.search(pattern, f.name)]

        if not files:
            print(f"   ℹ️  Aucun fichier trouvé")
            continue
        
        dates = [int(re.search(pattern, f.name).group(2)) for f in files]
        max_date = max(dates)
        files_to_delete = [f for f, d in zip(files, dates) if d < max_date]

        print(files_to_delete)
        
        for file in files_to_delete:
            print(f"   🗑️  {file.name}")
            # file.unlink()
        
        print(f"   📊 {len(files_to_delete)} supprimé(s)")

        
