import pandas as pd
from pathlib import Path
import xarray as xr
from art import tprint

from .clean import clean


def create_netcdf(file, CONVERT_DIR):
    var = file.stem.split('_QUOT_SIM2')[0]
    print(f"\n🌐 Conversion NetCDF: {file.name}")
    print(f"   → variable: {var}")

    data = pd.read_parquet(file)
    data = data.rename(columns={"LAMBX": "L2_X", "LAMBY": "L2_Y", "DATE": "time"})
    data['L2_X'] = data['L2_X'] * 100
    data['L2_Y'] = data['L2_Y'] * 100
    data['time'] = pd.to_datetime(data['time'], format='%Y%m%d')

    print(f"   → {len(data['time'].unique())} pas de temps | {data['L2_X'].nunique()}x{data['L2_Y'].nunique()} points de grille")

    ds = (data.set_index(['time', 'L2_Y', 'L2_X'])
              .to_xarray()
              .rename({'L2_Y': 'y', 'L2_X': 'x'}))
    ds.attrs['crs'] = 'EPSG:27572'

    output_file = CONVERT_DIR / file.with_suffix('.nc').name
    encoding = {
        var: {'zlib': True, 'complevel': 4, 'dtype': 'float32'},
        'time': {'units': 'days since 1970-01-01 00:00:00', 'calendar': 'standard', 'dtype': 'float64'}
    }

    ds.to_netcdf(output_file, encoding=encoding, unlimited_dims=['time'])

    print(f"   💾 {output_file.name}")
    return output_file


def convert(SPLIT_DIR, CONVERT_DIR, splited_files=None):
    """
    Convertit les fichiers Parquet en fichiers NetCDF géoréférencés.

    Args:
        SPLIT_DIR (str | Path):            Dossier contenant les fichiers Parquet.
        CONVERT_DIR (str | Path):          Dossier de sortie pour les fichiers NetCDF.
                                           Créé automatiquement s'il n'existe pas.
        splited_files (list[Path], optional): Fichiers Parquet à convertir.
                                              Si None, traite tous les *.parquet de SPLIT_DIR.

    Returns:
        list[Path]: Chemins des fichiers NetCDF créés.
                    Ex: [CONVERT_DIR/T_QUOT_SIM2_1958-1959.nc, ...]

    Notes:
        - CRS : EPSG:27572 (Lambert II étendu).
        - Compression : zlib niveau 4, variables en float32, time en float64.
    """
    
    tprint("convert", "small")
    
    SPLIT_DIR = Path(SPLIT_DIR)
    CONVERT_DIR = Path(CONVERT_DIR)
    CONVERT_DIR.mkdir(parents=True, exist_ok=True)

    if splited_files is None:
        splited_files = list(Path(SPLIT_DIR).glob("*.parquet"))

    print("CONVERSION")
        
    converted_files = []
    for file in splited_files:
        output_file = create_netcdf(file, CONVERT_DIR)       
        converted_files.append(output_file)

    clean(CONVERT_DIR)
        
    print("\nRÉSUMÉ")
    print(f"   - {len(converted_files)} fichier(s) converti(s)")
    print(f"   - 📁 Dossier: {os.path.abspath(CONVERT_DIR)}")
        
    return converted_files
