import re


def parse_filename(name: str) -> dict | None:
    """
    Parse un nom de fichier SIM2.
    Ex: PRENEI_QUOT_SIM2_historical-19580801-20191231.nc
    """
    pattern = r'^(?P<variable>.+)_QUOT_SIM2_(?P<version>latest|previous|historical)-(?P<date_debut>\d{8})-(?P<date_fin>\d{8})\.nc$'
    match = re.match(pattern, name)
    if not match:
        return None
    return match.groupdict()
