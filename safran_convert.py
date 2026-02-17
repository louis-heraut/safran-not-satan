import pandas as pd
from pathlib import Path
import pyproj
import numpy as np
import xarray as xr


def create_netcdf(file, CONVERT_DIR):
    var = file.stem.split('_QUOT_SIM2')[0]
    data = pd.read_parquet(file)
    data = data.rename(columns={
        "LAMBX": "L2_X",
        "LAMBY": "L2_Y",
        "DATE": "time"
    })
    data['L2_X'] = data['L2_X'] * 100
    data['L2_Y'] = data['L2_Y'] * 100
    data['time'] = pd.to_datetime(data['time'], format='%Y%m%d')
    
    # Conversion directe pandas → xarray
    ds = (data.set_index(['time', 'L2_Y', 'L2_X'])
              .to_xarray()
              .rename({'L2_Y': 'y', 'L2_X': 'x'}))
    
    # Métadonnées minimales
    ds.attrs['crs'] = 'EPSG:27572'
    
    output_file = CONVERT_DIR / file.with_suffix('.nc').name
    
    # Encodage
    encoding = {
        var: {
            'zlib': True,
            'complevel': 4,
            'dtype': 'float32'
        },
        'time': {
            'units': 'days since 1970-01-01 00:00:00',
            'calendar': 'standard',
            'dtype': 'float64'
        }
    }
    
    ds.to_netcdf(
        output_file,
        encoding=encoding,
        unlimited_dims=['time']
    )
    
    return output_file


def convert_files(SPLIT_DIR, CONVERT_DIR, splited_files=None):
    SPLIT_DIR = Path(SPLIT_DIR)
    CONVERT_DIR = Path(CONVERT_DIR)
    CONVERT_DIR.mkdir(parents=True, exist_ok=True)

    if splited_files is None:
        splited_files = list(Path(SPLIT_DIR).glob("*.parquet"))

    converted_files = []
    for file in splited_files:
        output_file = create_netcdf(file, CONVERT_DIR)       
        converted_files.append(output_file)
        
    return converted_files
