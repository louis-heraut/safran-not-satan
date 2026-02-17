import pandas as pd
from pathlib import Path
import xarray as xr


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


def concatenate_nc_files(files, output_file):
    import subprocess
    subprocess.run(['ncrcat', '-O'] + [str(f) for f in files] + [str(output_file)])


    
def merge_historical_files(CONVERT_DIR, OUTPUT_DIR, converted_files):

    converted_historical_files = get_historical_files(converted_files)
    if len(converted_historical_files) == 0:
        return

    all_files = list(CONVERT_DIR.glob("*.nc"))
    all_files = get_historical_files(all_files)
    all_variables = get_variables(all_files)
    
    converted_variables = get_set_variables(converted_historical_files)
    
    all_files_to_update = [file for file, var in zip(all_files, all_variables) if var in converted_variables]
    all_variables_to_update = get_variables(all_files_to_update)

    variables = get_set_variables(all_files_to_update)
    
    for var in variables:
        var_files = [f for f, v in zip(all_files_to_update, all_variables_to_update) if v == var]
    
        tmp_file = OUTPUT_DIR / f"{var}_QUOT_SIM2_historical_tmp.nc"
        concatenate_nc_files(var_files, tmp_file)
    
        ds = xr.open_dataset(tmp_file)
        min_date = ds.time.min().dt.strftime('%Y%m%d').values
        max_date = ds.time.max().dt.strftime('%Y%m%d').values
        ds.close()
    
        period = f"{min_date}-{max_date}"
        output_file = OUTPUT_DIR / f"{var}_QUOT_SIM2_historical-{period}.nc"
        tmp_file.rename(output_file)



def merge_previous_files(CONVERT_DIR, OUTPUT_DIR, converted_files):

    all_historical_files = list(OUTPUT_DIR.glob("*historical*.nc"))
    all_historical_variables = get_variables(all_historical_files)

    converted_previous_files = get_previous_files(converted_files)
    if len(converted_previous_files) == 0:
        return

    all_files = list(CONVERT_DIR.glob("*.nc"))
    all_files = get_previous_files(all_files)
    all_variables = get_variables(all_files)

    converted_variables = get_set_variables(converted_previous_files)
    
    all_files_to_update = [file for file, var in zip(all_files, all_variables) if var in converted_variables]
    all_variables_to_update = get_variables(all_files_to_update)

    variables = get_set_variables(all_files_to_update)
    
    for var in variables:
        var_historical_file = [f for f, v in zip(all_historical_files, all_historical_variables) if v == var]
        var_file = [f for f, v in zip(all_files_to_update, all_variables_to_update) if v == var]
        var_files = var_historical_file + var_file
    
        tmp_file = OUTPUT_DIR / f"{var}_QUOT_SIM2_previous_tmp.nc"
        concatenate_nc_files(var_files, tmp_file)
    
        ds = xr.open_dataset(tmp_file)
        min_date = ds.time.min().dt.strftime('%Y%m%d').values
        max_date = ds.time.max().dt.strftime('%Y%m%d').values
        ds.close()
    
        period = f"{min_date}-{max_date}"
        output_file = OUTPUT_DIR / f"{var}_QUOT_SIM2_previous-{period}.nc"
        tmp_file.rename(output_file)


def merge_latest_files(CONVERT_DIR, OUTPUT_DIR, converted_files):

    all_previous_files = list(OUTPUT_DIR.glob("*previous*.nc"))
    all_previous_variables = get_variables(all_previous_files)

    converted_latest_files = get_latest_files(converted_files)
    if len(converted_latest_files) == 0:
        return

    all_files = list(CONVERT_DIR.glob("*.nc"))
    all_files = get_latest_files(all_files)
    all_variables = get_variables(all_files)

    converted_variables = get_set_variables(converted_latest_files)
    
    all_files_to_update = [file for file, var in zip(all_files, all_variables) if var in converted_variables]
    all_variables_to_update = get_variables(all_files_to_update)

    variables = get_set_variables(all_files_to_update)
    
    for var in variables:
        var_previous_file = [f for f, v in zip(all_previous_files, all_previous_variables) if v == var]
        var_file = [f for f, v in zip(all_files_to_update, all_variables_to_update) if v == var]
        var_files = var_previous_file + var_file
    
        tmp_file = OUTPUT_DIR / f"{var}_QUOT_SIM2_latest_tmp.nc"
        concatenate_nc_files(var_files, tmp_file)
    
        ds = xr.open_dataset(tmp_file)
        min_date = ds.time.min().dt.strftime('%Y%m%d').values
        max_date = ds.time.max().dt.strftime('%Y%m%d').values
        ds.close()
    
        period = f"{min_date}-{max_date}"
        output_file = OUTPUT_DIR / f"{var}_QUOT_SIM2_latest-{period}.nc"
        tmp_file.rename(output_file)



def merge_files(SPLIT_DIR, OUTPUT_DIR, converted_files=None):
    CONVERT_DIR = Path(CONVERT_DIR)
    OUTPUT_DIR = Path(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if converted_files is None:
        converted_files = list(Path(CONVERT_DIR).glob("*.nc"))
    
    merge_historical_files(CONVERT_DIR, OUTPUT_DIR, converted_files)
    merge_previous_files(CONVERT_DIR, OUTPUT_DIR, converted_files)
    merge_latest_files(CONVERT_DIR, OUTPUT_DIR, converted_files) 





