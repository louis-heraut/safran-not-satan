import os
import requests
import json
import time
import math
import pandas as pd
from pathlib import Path
from art import tprint
import boto3
from datetime import datetime, timezone

from .tools import parse_filename


def safe_str(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return ""
    return str(val)


def generate_index(S3_BUCKET: str,
                   S3_PREFIX: str = "",
                   METADATA_VARIABLES_FILE: str = None,
                   INDEX_PATH: str = "data-access.html",
                   S3_ACCESS_KEY: str = None,
                   S3_SECRET_KEY: str = None,
                   S3_ENDPOINT: str = None,
                   S3_REGION: str = None):
    """
    Génère un fichier HTML listant les liens S3 groupés par variable et type.
    Les fichiers NC sont listés directement depuis le bucket S3.
    """

    # Lister les fichiers NC depuis S3
    s3 = boto3.client('s3',
                      aws_access_key_id=S3_ACCESS_KEY,
                      aws_secret_access_key=S3_SECRET_KEY,
                      endpoint_url=S3_ENDPOINT,
                      region_name=S3_REGION)

    paginator = s3.get_paginator('list_objects_v2')
    file_names = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.nc'):
                file_names.append(Path(key).name)

    if not file_names:
        print("\n⚠️  Aucun fichier NC trouvé sur S3")
        return None

    # Base URL S3
    if S3_ENDPOINT:
        base_url = f"{S3_ENDPOINT.rstrip('/')}/{S3_BUCKET}"
    else:
        base_url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com"
    if S3_PREFIX:
        base_url += f"/{S3_PREFIX.strip('/')}"

    # Charger les métadonnées
    var_meta = {}
    if METADATA_VARIABLES_FILE and Path(METADATA_VARIABLES_FILE).exists():
        df = pd.read_csv(METADATA_VARIABLES_FILE, index_col='variable')
        var_meta = df.to_dict(orient='index')

    # Grouper par variable et type
    from collections import defaultdict
    grouped = defaultdict(lambda: defaultdict(list))
    type_order = ['latest', 'previous', 'historical']

    for name in file_names:
        parts = name.split('_QUOT_SIM2_')
        if len(parts) != 2:
            continue
        variable = parts[0]
        file_type = parts[1].split('-')[0]
        url = f"{base_url}/{name}"
        grouped[variable][file_type].append((name, url))

    # Générer le HTML
    lines = []
    lines.append('<h2>Données SIM : SAFRAN-ISBA-MODCOU — Accès aux fichiers</h2>')
    lines.append('<p>Les fichiers sont au format NetCDF (.nc) et hébergés sur un stockage objet S3. '
                 'Les liens <strong>latest</strong> pointent vers la version la plus récente.</p>')
    lines.append('<hr>')

    for variable in sorted(grouped.keys()):
        meta = var_meta.get(variable, {})
        description = meta.get('description', '')
        unite = meta.get('unite', '')
        periode = meta.get('periode_agregation', '')

        lines.append(f'<h3>{variable}</h3>')
        if description:
            lines.append(f'<p><strong>{description}</strong>')
            details = []
            if unite:
                details.append(f'Unité : {unite}')
            if periode:
                details.append(f"Période d'agrégation : {periode}")
            if details:
                lines.append(f'<br>{"  —  ".join(details)}')
            lines.append('</p>')

        for file_type in type_order:
            if file_type not in grouped[variable]:
                continue
            label = {
                'latest':     '<i>Latest</i> — update journalière',
                'previous':   '<i>Previous</i> — update mensuelle',
                'historical': '<i>Historical</i> — update décénale'
            }.get(file_type, file_type)
            lines.append(f'<p>{label}<br>')
            for name, url in sorted(grouped[variable][file_type]):
                lines.append(f'<a href="{url}">{name}</a><br>')
            lines.append('</p>')
        lines.append('<hr>')

    html = '\n'.join(lines)
    with open(INDEX_PATH, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"✅ HTML généré : {INDEX_PATH} ({len(grouped)} variables)")
    return INDEX_PATH


# def generate_stac_catalog(CATALOG_DIR,
#                           S3_BUCKET: str,
#                           S3_PREFIX: str = "",
#                           METADATA_VARIABLES_FILE: str = None,
#                           S3_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY"),
#                           S3_SECRET_KEY: str = os.getenv("S3_SECRET_KEY"),
#                           S3_ENDPOINT: str = os.getenv("S3_ENDPOINT"),
#                           S3_REGION: str = os.getenv("S3_REGION", "eu-west-1")):

#     # Base URL racine du bucket
#     if S3_ENDPOINT:
#         base_url = f"{S3_ENDPOINT.rstrip('/')}/{S3_BUCKET}"
#     else:
#         base_url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com"

#     # URL des données (avec prefix)
#     if S3_PREFIX:
#         data_base_url = f"{base_url}/{S3_PREFIX.strip('/')}"
#     else:
#         data_base_url = base_url

#     # Nom du jeu de données = dernier segment du prefix (ex: "safran-fairy")
#     dataset_name = S3_PREFIX.strip('/').split('/')[-1] if S3_PREFIX else "dataset"

#     # URLs STAC fixes, indépendantes du prefix
#     stac_base_url    = f"{base_url}/stac-data/{dataset_name}"
#     stac_catalog_url = f"{base_url}/stac-data/catalog.json"

#     # Lister les fichiers depuis S3
#     s3 = boto3.client('s3',
#                       aws_access_key_id=S3_ACCESS_KEY,
#                       aws_secret_access_key=S3_SECRET_KEY,
#                       endpoint_url=S3_ENDPOINT,
#                       region_name=S3_REGION)

#     all_keys = []
#     paginator = s3.get_paginator('list_objects_v2')
#     for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
#         for obj in page.get('Contents', []):
#             all_keys.append(obj['Key'])

#     # Métadonnées variables
#     var_meta = {}
#     if METADATA_VARIABLES_FILE and Path(METADATA_VARIABLES_FILE).exists():
#         df = pd.read_csv(METADATA_VARIABLES_FILE, index_col='variable')
#         var_meta = df.to_dict(orient='index')

#     # Grouper par (variable, version) en gardant le plus récent
#     from collections import defaultdict
#     grouped = defaultdict(dict)
#     for key in all_keys:
#         filename = Path(key).name
#         if not filename.endswith('.nc'):
#             continue
#         parsed = parse_filename(filename)
#         if not parsed:
#             continue
#         variable = parsed['variable']
#         version  = parsed['version']
#         date_fin = int(parsed['date_fin'])
#         existing = grouped[variable].get(version)
#         if existing is None or date_fin > existing['date_fin_int']:
#             grouped[variable][version] = {
#                 'filename':     filename,
#                 'url':          f"{base_url}/{key}",
#                 'date_debut':   parsed['date_debut'],
#                 'date_fin':     parsed['date_fin'],
#                 'date_fin_int': date_fin
#             }

#     if not grouped:
#         print("⚠️  Aucun fichier reconnu dans le bucket")
#         return []

#     # Créer l'arborescence locale
#     catalog_dir = Path(CATALOG_DIR)
#     items_dir   = catalog_dir / "items"
#     catalog_dir.mkdir(exist_ok=True)
#     items_dir.mkdir(exist_ok=True)

#     def fmt_date(d):
#         return f"{d[:4]}-{d[4:6]}-{d[6:8]}T00:00:00Z"

#     # Description par version
#     version_descriptions = {
#         'historical': "Chronique historique excluant la dernière décennie (stable).",
#         'previous':   "Chronique historique comprenant la décennie en cours jusqu'au dernier mois écoulé (mise à jour mensuelle).",
#         'latest':     "Chronique historique comprenant la décennie en cours jusqu'au dernier jour écoulé (mise à jour quotidienne).",
#     }

#     BBOX = [-4.962155, 42.348763, 8.183832, 51.049739]

#     output_files = []
#     item_links   = []

#     for variable in sorted(grouped.keys()):
#         meta = var_meta.get(variable, {})
#         for version, f in sorted(grouped[variable].items()):
#             item_id = f"{variable}_SIM2_{version}"
#             item = {
#                 "type":         "Feature",
#                 "stac_version": "1.0.0",
#                 "id":           item_id,
#                 "geometry": {
#                     "type": "Polygon",
#                     "coordinates": [[
#                         [BBOX[0], BBOX[1]],
#                         [BBOX[2], BBOX[1]],
#                         [BBOX[2], BBOX[3]],
#                         [BBOX[0], BBOX[3]],
#                         [BBOX[0], BBOX[1]]
#                     ]]
#                 },
#                 "bbox": BBOX,
#                 "properties": {
#                     "datetime":           None,
#                     "title":              f['filename'],
#                     "start_datetime":     fmt_date(f['date_debut']),
#                     "end_datetime":       fmt_date(f['date_fin']),
#                     "variable":           variable,
#                     "version_type":       version,
#                     "description":        safe_str(meta.get('description')) or version_descriptions.get(version, ""),
#                     "unite":              safe_str(meta.get('unite')),
#                     "periode_agregation": safe_str(meta.get('periode_agregation')),
#                     "license":            "etalab-2.0",
#                     "doi":                "10.57745/BAZ12C",
#                 },
#                 "assets": {
#                     "data": {
#                         "href":  f['url'],
#                         "type":  "application/x-netcdf",
#                         "title": f['filename'],
#                         "roles": ["data"]
#                     }
#                 },
#                 "links": [
#                     {"rel": "root",       "href": stac_catalog_url,                          "type": "application/json"},
#                     {"rel": "collection", "href": f"{stac_base_url}/collection.json",        "type": "application/json"},
#                     {"rel": "self",       "href": f"{stac_base_url}/items/{item_id}.json",   "type": "application/json"},
#                     {"rel": "cite-as",    "href": "https://doi.org/10.57745/BAZ12C"},
#                     {"rel": "license",    "href": "https://www.etalab.gouv.fr/licence-ouverte-open-licence"}
#                 ]
#             }

#             item_path = items_dir / f"{item_id}.json"
#             with open(item_path, 'w', encoding='utf-8') as fp:
#                 json.dump(item, fp, ensure_ascii=False, indent=2)
#             output_files.append(item_path)

#             item_links.append({
#                 "rel":   "item",
#                 "href":  f"{stac_base_url}/items/{item_id}.json",
#                 "type":  "application/json",
#                 "title": f['filename']
#             })

#     # Collection
#     collection = {
#         "type":         "Collection",
#         "id":           f"sim2-{dataset_name}",
#         "stac_version": "1.0.0",
#         "title":        "SIM2 — Données de réanalyse hydro-météorologique quotidienne (SAFRAN-ISBA-MODCOU)",
#         "description":  (
#             "Données quotidiennes de réanalyse atmosphérique et bilan hydrique sur la France métropolitaine "
#             "selon une grille spatiale de 8 km. Composante de surface de la chaîne hydrométéorologique SIM "
#             "(SAFRAN-ISBA-MODCOU) développée par Météo-France/CNRM. "
#             "Source : Météo-France, 2026, https://doi.org/10.57745/BAZ12C"
#         ),
#         "license": "etalab-2.0",
#         "extent": {
#             "spatial":  {"bbox": [BBOX]},
#             "temporal": {"interval": [["1958-08-01T00:00:00Z", None]]}
#         },
#         "keywords": [
#             "réanalyse atmosphérique",
#             "simulation climatique",
#             "continental surface model",
#             "atmospheric forcing",
#             "climate change",
#             "SAFRAN",
#             "SIM2"
#         ],
#         "providers": [
#             {
#                 "name":  "Météo-France / CNRM",
#                 "roles": ["producer", "licensor"],
#                 "url":   "https://www.meteo.fr"
#             },
#             {
#                 "name":  "INRAE / RiverLy",
#                 "roles": ["host"],
#                 "url":   "https://www.inrae.fr"
#             }
#         ],
#         "links": [
#             {"rel": "root",     "href": stac_catalog_url,                   "type": "application/json"},
#             {"rel": "self",     "href": f"{stac_base_url}/collection.json", "type": "application/json"},
#             {"rel": "parent",   "href": stac_catalog_url,                   "type": "application/json"},
#             {"rel": "cite-as",  "href": "https://doi.org/10.57745/BAZ12C"},
#             {"rel": "license",  "href": "https://www.etalab.gouv.fr/licence-ouverte-open-licence"},
#             *item_links
#         ]
#     }

#     collection_path = catalog_dir / "collection.json"
#     with open(collection_path, 'w', encoding='utf-8') as fp:
#         json.dump(collection, fp, ensure_ascii=False, indent=2)
#     output_files.append(collection_path)

#     print(f"✅ STAC généré : {len(output_files) - 1} items, {len(grouped)} variables")
#     print(f"   → {catalog_dir}/collection.json")
#     print(f"   → {catalog_dir}/items/ ({len(output_files) - 1} fichiers)")
#     return output_files


def generate_stac_catalog(CATALOG_DIR,
                          S3_BUCKET: str,
                          S3_PREFIX: str = "",
                          METADATA_VARIABLES_FILE: str = None,
                          S3_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY"),
                          S3_SECRET_KEY: str = os.getenv("S3_SECRET_KEY"),
                          S3_ENDPOINT: str = os.getenv("S3_ENDPOINT"),
                          S3_REGION: str = os.getenv("S3_REGION", "eu-west-1")):

    # Base URL racine du bucket
    if S3_ENDPOINT:
        base_url = f"{S3_ENDPOINT.rstrip('/')}/{S3_BUCKET}"
    else:
        base_url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com"

    # Nom du jeu de données = dernier segment du prefix (ex: "safran-fairy")
    dataset_name = S3_PREFIX.strip('/').split('/')[-1] if S3_PREFIX else "dataset"

    # URLs STAC
    stac_base_url        = f"{base_url}/stac-data/{dataset_name}"
    stac_catalog_url     = f"{base_url}/stac-data/catalog.json"
    stac_collection_url  = f"{stac_base_url}/collection.json"

    # Lister les fichiers depuis S3
    s3 = boto3.client('s3',
                      aws_access_key_id=S3_ACCESS_KEY,
                      aws_secret_access_key=S3_SECRET_KEY,
                      endpoint_url=S3_ENDPOINT,
                      region_name=S3_REGION)

    all_keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get('Contents', []):
            all_keys.append(obj['Key'])

    # Métadonnées variables
    var_meta = {}
    if METADATA_VARIABLES_FILE and Path(METADATA_VARIABLES_FILE).exists():
        df = pd.read_csv(METADATA_VARIABLES_FILE, index_col='variable')
        var_meta = df.to_dict(orient='index')

    # Grouper par (variable, version) en gardant le plus récent
    from collections import defaultdict
    grouped = defaultdict(dict)
    for key in all_keys:
        filename = Path(key).name
        if not filename.endswith('.nc'):
            continue
        parsed = parse_filename(filename)
        if not parsed:
            continue
        variable = parsed['variable']
        version  = parsed['version']
        date_fin = int(parsed['date_fin'])
        existing = grouped[variable].get(version)
        if existing is None or date_fin > existing['date_fin_int']:
            grouped[variable][version] = {
                'filename':     filename,
                'url':          f"{base_url}/{key}",
                'date_debut':   parsed['date_debut'],
                'date_fin':     parsed['date_fin'],
                'date_fin_int': date_fin
            }

    if not grouped:
        print("⚠️  Aucun fichier reconnu dans le bucket")
        return []

    # Créer l'arborescence locale
    catalog_dir = Path(CATALOG_DIR)
    catalog_dir.mkdir(exist_ok=True)

    def fmt_date(d):
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}T00:00:00Z"

    version_descriptions = {
        'historical': "Mise à jour décennale",
        'previous':   "Mise à jour mensuelle",
        'latest':     "Mise à jour quotidienne",
    }

    BBOX = [-4.962155, 42.348763, 8.183832, 51.049739]

    output_files    = []
    child_links     = []  # liens vers les sous-collections dans la collection mère

    # ── Générer une sous-collection + items par variable ──────────────
    for variable in sorted(grouped.keys()):
        meta = var_meta.get(variable, {})

        # Dossier de la variable
        var_dir   = catalog_dir / variable
        items_dir = var_dir / "items"
        var_dir.mkdir(exist_ok=True)
        items_dir.mkdir(exist_ok=True)

        stac_var_url = f"{stac_base_url}/{variable}"
        item_links   = []

        for version, f in sorted(grouped[variable].items()):
            item_id = f"{variable}_SIM2_{version}"
            version_description = version_descriptions.get(version, "")
            item_description = (
                f"[{version_description}] {safe_str(meta.get('description'))}"
            )
            item = {
                "type":         "Feature",
                "stac_version": "1.0.0",
                "id":           item_id,
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [BBOX[0], BBOX[1]],
                        [BBOX[2], BBOX[1]],
                        [BBOX[2], BBOX[3]],
                        [BBOX[0], BBOX[3]],
                        [BBOX[0], BBOX[1]]
                    ]]
                },
                "bbox": BBOX,
                "properties": {
                    "datetime":           None,
                    "title":              f['filename'],
                    "start_datetime":     fmt_date(f['date_debut']),
                    "end_datetime":       fmt_date(f['date_fin']),
                    "variable":           variable,
                    "version_type":       version,
                    "description":        item_description,
                    "unite":              safe_str(meta.get('unite')),
                    "periode_agregation": safe_str(meta.get('periode_agregation')),
                    "license":            "etalab-2.0",
                    "doi":                "10.57745/BAZ12C",
                },
                "assets": {
                    "data": {
                        "href":  f['url'],
                        "type":  "application/x-netcdf",
                        "title": f['filename'],
                        "roles": ["data"]
                    }
                },
                "links": [
                    {"rel": "root",       "href": stac_catalog_url,                              "type": "application/json"},
                    {"rel": "parent",     "href": f"{stac_var_url}/collection.json",             "type": "application/json"},
                    {"rel": "collection", "href": f"{stac_var_url}/collection.json",             "type": "application/json"},
                    {"rel": "self",       "href": f"{stac_var_url}/items/{item_id}.json",        "type": "application/json"},
                    {"rel": "cite-as",    "href": "https://doi.org/10.57745/BAZ12C"},
                    {"rel": "license",    "href": "https://www.etalab.gouv.fr/licence-ouverte-open-licence"}
                ]
            }

            item_path = items_dir / f"{item_id}.json"
            with open(item_path, 'w', encoding='utf-8') as fp:
                json.dump(item, fp, ensure_ascii=False, indent=2)
            output_files.append(item_path)

            item_links.append({
                "rel":   "item",
                "href":  f"{stac_var_url}/items/{item_id}.json",
                "type":  "application/json",
                "title": f['filename']
            })

        # Sous-collection de la variable
        var_description = safe_str(meta.get('description')) or f"Variable {variable} — SIM2 SAFRAN-ISBA-MODCOU"
        sub_collection = {
            "type":         "Collection",
            "id":           f"sim2-{dataset_name}-{variable}",
            "stac_version": "1.0.0",
            "title":        f"{variable} — {safe_str(meta.get('description')) or variable}",
            "description":  var_description,
            "license":      "etalab-2.0",
            "extent": {
                "spatial":  {"bbox": [BBOX]},
                "temporal": {"interval": [[
                    fmt_date(min(f['date_debut'] for f in grouped[variable].values())),
                    fmt_date(max(f['date_fin']   for f in grouped[variable].values()))
                ]]}
            },
            "links": [
                {"rel": "root",     "href": stac_catalog_url,          "type": "application/json"},
                {"rel": "parent",   "href": stac_collection_url,        "type": "application/json"},
                {"rel": "self",     "href": f"{stac_var_url}/collection.json", "type": "application/json"},
                {"rel": "cite-as",  "href": "https://doi.org/10.57745/BAZ12C"},
                {"rel": "license",  "href": "https://www.etalab.gouv.fr/licence-ouverte-open-licence"},
                *item_links
            ]
        }

        sub_collection_path = var_dir / "collection.json"
        with open(sub_collection_path, 'w', encoding='utf-8') as fp:
            json.dump(sub_collection, fp, ensure_ascii=False, indent=2)
        output_files.append(sub_collection_path)

        child_links.append({
            "rel":   "child",
            "href":  f"{stac_var_url}/collection.json",
            "type":  "application/json",
            "title": f"{variable} — {safe_str(meta.get('description')) or variable}"
        })

    # ── Collection mère ────────────────────────────────────────────────
    collection = {
        "type":         "Collection",
        "id":           f"sim2-{dataset_name}",
        "stac_version": "1.0.0",
        "title":        "SIM2 — Données de réanalyse hydro-météorologique quotidienne (SAFRAN-ISBA-MODCOU)",
        "description":  (
            "Données quotidiennes de réanalyse atmosphérique et bilan hydrique sur la France métropolitaine "
            "selon une grille spatiale de 8 km. Composante de surface de la chaîne hydrométéorologique SIM "
            "(SAFRAN-ISBA-MODCOU) développée par Météo-France/CNRM. "
            "Source : Météo-France, 2026, https://doi.org/10.57745/BAZ12C"
        ),
        "license": "etalab-2.0",
        "extent": {
            "spatial":  {"bbox": [BBOX]},
            "temporal": {"interval": [["1958-08-01T00:00:00Z", None]]}
        },
        "keywords": [
            "réanalyse atmosphérique",
            "simulation climatique",
            "continental surface model",
            "atmospheric forcing",
            "climate change",
            "SAFRAN",
            "SIM2"
        ],
        "providers": [
            {
                "name":  "Météo-France / CNRM",
                "roles": ["producer", "licensor"],
                "url":   "https://www.meteo.fr"
            },
            {
                "name":  "INRAE / RiverLy",
                "roles": ["host"],
                "url":   "https://www.inrae.fr"
            }
        ],
        "links": [
            {"rel": "root",    "href": stac_catalog_url,     "type": "application/json"},
            {"rel": "self",    "href": stac_collection_url,  "type": "application/json"},
            {"rel": "parent",  "href": stac_catalog_url,     "type": "application/json"},
            {"rel": "cite-as", "href": "https://doi.org/10.57745/BAZ12C"},
            {"rel": "license", "href": "https://www.etalab.gouv.fr/licence-ouverte-open-licence"},
            *child_links
        ]
    }

    collection_path = catalog_dir / "collection.json"
    with open(collection_path, 'w', encoding='utf-8') as fp:
        json.dump(collection, fp, ensure_ascii=False, indent=2)
    output_files.append(collection_path)

    total_items = sum(len(v) for v in grouped.values())
    print(f"✅ STAC généré : {len(grouped)} variables, {total_items} items")
    print(f"   → {catalog_dir}/collection.json")
    print(f"   → {catalog_dir}/{{variable}}/collection.json  (x{len(grouped)})")
    print(f"   → {catalog_dir}/{{variable}}/items/  ({total_items} fichiers)")
    return output_files


# La structure générée localement sera :
# CATALOG_DIR/
# ├── collection.json          ← collection mère
# ├── DLI/
# │   ├── collection.json      ← sous-collection
# │   └── items/
# │       ├── DLI_SIM2_historical.json
# │       ├── DLI_SIM2_latest.json
# │       └── DLI_SIM2_previous.json
# ├── ETP/
# │   ├── collection.json
# │   └── items/
# └── ...
