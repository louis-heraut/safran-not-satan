import os
import pandas as pd
from pathlib import Path
import xarray as xr
from art import tprint
from datetime import datetime, timedelta

from .clean import clean_local


def get_historical_files(files):
    historical_files = [f for f in files if "latest" not in f.name and "previous" not in f.name]
    historical_files = sorted(historical_files)
    return historical_files

def get_previous_files(files):
    previous_files = [f for f in files if "previous" in f.name]
    previous_files = sorted(previous_files)
    return previous_files

def get_latest_files(files):
    latest_files = [f for f in files if "latest" in f.name]
    latest_files = sorted(latest_files)
    return latest_files


def get_variables(files):
    variables = [f.stem.split('_QUOT_SIM2')[0] for f in files]
    return variables

def get_set_variables(files):
    variables = get_variables(files)
    variables = sorted(list(set(variables)))
    return variables


def concatenate_nc_files(files, output_file, cutoff_date=None):
    import subprocess
    
    if cutoff_date is None:
        subprocess.run(['ncrcat', '-h', '-O'] + [str(f) for f in files] + [str(output_file)])
    else:
        base_files = files[:-1]
        new_files = files[-1:]
        
        cutoff_exclusive = (datetime.strptime(cutoff_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        
        subprocess.run(
            ['ncrcat', '-h', '-O', '-d', f'time,,{cutoff_exclusive}']
            + [str(f) for f in base_files]
            + [str(output_file)]
        )
        subprocess.run(
            ['ncrcat', '-h', '-A', str(output_file)]
            + [str(f) for f in new_files]
            + [str(output_file)]
        )
        

def merge_by_type(file_type, source_getter, base_getter, CONVERT_DIR, OUTPUT_DIR, converted_files):
    """
    Fusionne les fichiers NetCDF d'un type donné (historical, previous, latest).

    Pour chaque variable concernée, concatène les fichiers de base existants
    avec les nouveaux fichiers convertis, puis renomme le résultat avec la
    plage temporelle effective.

    Args:
        file_type (str):              Type de fichier. Ex: 'historical', 'previous', 'latest'
        source_getter (callable):     Fonction filtrant les fichiers du bon type.
                                      Ex: get_historical_files
        base_getter (callable | None): Fonction retournant les fichiers de base depuis OUTPUT_DIR.
                                       None si pas de base (cas historical).
        CONVERT_DIR (Path):           Dossier contenant tous les fichiers NetCDF convertis.
        OUTPUT_DIR (Path):            Dossier de sortie pour les fichiers mergés.
        converted_files (list[Path]): Fichiers NetCDF nouvellement convertis à intégrer.

    Returns:
        list[Path] | None: Fichiers NetCDF mergés, ou None si aucun fichier du type trouvé.
                           Ex: [OUTPUT_DIR/T_QUOT_SIM2_historical-19580101-19991231.nc, ...]

    Notes:
        - Utilise ncrcat (NetCDF Operators) pour la concaténation temporelle.
        - Le fichier de sortie est nommé avec les dates min/max réelles de la série.
        - Passe par un fichier temporaire _tmp.nc renommé après vérification des dates.
    """
    
    converted_type_files = source_getter(converted_files)
    if len(converted_type_files) == 0:
        return []

    all_files = source_getter(list(CONVERT_DIR.glob("*.nc")))
    all_variables = get_variables(all_files)
    converted_variables = get_set_variables(converted_type_files)

    files_to_update = [f for f, v in zip(all_files, all_variables) if v in converted_variables]
    variables_to_update = get_variables(files_to_update)

    base_files = base_getter(OUTPUT_DIR) if base_getter else []
    base_variables = get_variables(base_files)

    unique_vars = get_set_variables(files_to_update)
    print(f"   → {len(unique_vars)} variable(s): {', '.join(unique_vars)}")
    print(f"   → {len(base_files)} fichier(s) de base + {len(files_to_update)} nouveau(x)")

    merged_files = []
    for i, var in enumerate(unique_vars, 1):
        print(f"\n[{i}/{len(unique_vars)}]")
    
        var_base = [f for f, v in zip(base_files, base_variables) if v == var]
        var_new = [f for f, v in zip(files_to_update, variables_to_update) if v == var]

        cutoff_date = None
        if file_type == 'latest' and var_base:
            cutoff_raw = var_new[0].stem.split('latest-')[1].split('-')[0]
            cutoff_date = f"{cutoff_raw[:4]}-{cutoff_raw[4:6]}-{cutoff_raw[6:8]}"
            print(f"   ✂️  Troncature previous avant {cutoff_date}")

        var_files = var_base + var_new

        if file_type == "historical":
            print(f"\n🧩 Merge {len(var_base)} historical NetCDF")
        else:
            print(f"\n🧩 Merge {len(var_base)} previous NetCDF and {len(var_new)} {file_type} NetCDF")
        print(f"   → variable: {var}")
        
        tmp_file = OUTPUT_DIR / f"{var}_QUOT_SIM2_{file_type}_tmp.nc"
        concatenate_nc_files(var_files, tmp_file, cutoff_date=cutoff_date)

        ds = xr.open_dataset(tmp_file)
        min_date = ds.time.min().dt.strftime('%Y%m%d').values
        max_date = ds.time.max().dt.strftime('%Y%m%d').values
        ds.close()

        output_file = OUTPUT_DIR / f"{var}_QUOT_SIM2_{file_type}-{min_date}-{max_date}.nc"
        tmp_file.rename(output_file)
        merged_files.append(output_file)
        print(f"   💾 {output_file.name}")
        
    return merged_files


def merge_historical(CONVERT_DIR, OUTPUT_DIR, converted_files):
    print(f"\nMERGE HISTORICAL")
    merged_files = merge_by_type('historical', get_historical_files,
                                 None,
                                 CONVERT_DIR, OUTPUT_DIR,
                                 converted_files)
    return merged_files

def merge_previous(CONVERT_DIR, OUTPUT_DIR, converted_files):
    print(f"\nMERGE PREVIOUS")
    merged_files = merge_by_type('previous', get_previous_files,
                                 lambda d: list(d.glob("*historical*.nc")),
                                 CONVERT_DIR, OUTPUT_DIR,
                                 converted_files)
    return merged_files
    
def merge_latest(CONVERT_DIR, OUTPUT_DIR, converted_files):
    print(f"\nMERGE LATEST")
    merged_files = merge_by_type('latest', get_latest_files,
                                 lambda d: list(d.glob("*previous*.nc")),
                                 CONVERT_DIR, OUTPUT_DIR,
                                 converted_files)
    return merged_files

    
def merge(CONVERT_DIR, OUTPUT_DIR, converted_files=None):
    """
    Orchestre la fusion des fichiers NetCDF par type (historical, previous, latest).

    Appelle successivement merge_historical(), merge_previous() et merge_latest(),
    puis nettoie les fichiers obsolètes dans OUTPUT_DIR.

    Args:
        CONVERT_DIR (str | Path):          Dossier contenant les fichiers NetCDF convertis.
        OUTPUT_DIR (str | Path):           Dossier de sortie pour les fichiers mergés.
                                           Créé automatiquement s'il n'existe pas.
        converted_files (list[Path], optional): Fichiers NetCDF à intégrer.
                                                Si None, traite tous les *.nc de CONVERT_DIR.

    Returns:
        list[Path]: Liste de tous les fichiers NetCDF mergés (historical + previous + latest).
    """

    tprint("merge", "small")
        
    CONVERT_DIR = Path(CONVERT_DIR)
    OUTPUT_DIR = Path(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if converted_files is None:
        converted_files = list(Path(CONVERT_DIR).glob("*.nc"))

    merged_historical_files = merge_historical(CONVERT_DIR, OUTPUT_DIR,
                                               converted_files)
    merged_previous_files = merge_previous(CONVERT_DIR, OUTPUT_DIR,
                                           converted_files)
    merged_latest_files = merge_latest(CONVERT_DIR, OUTPUT_DIR,
                                       converted_files)
    merged_files = merged_historical_files + merged_previous_files + merged_latest_files 
    
    print(f"\nRÉSUMÉ")
    print(f"   - {len(merged_files)} fichier(s) mergés")
    print(f"   - 📁 Dossier: {os.path.abspath(OUTPUT_DIR)}")
    
    return merged_files
