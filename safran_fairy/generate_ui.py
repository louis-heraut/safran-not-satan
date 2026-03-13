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


def generate_stac_catalog(CATALOG_DIR,
                          S3_BUCKET: str,
                          S3_PREFIX: str = "",
                          METADATA_VARIABLES_FILE: str = None,
                          S3_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY"),
                          S3_SECRET_KEY: str = os.getenv("S3_SECRET_KEY"),
                          S3_ENDPOINT: str = os.getenv("S3_ENDPOINT"),
                          S3_REGION: str = os.getenv("S3_REGION", "eu-west-1")):
    """
    Génère un catalogue STAC minimal valide pour SIM2.
    Liste les fichiers directement depuis S3.
    Produit : stac-data/catalog.json + collection.json + items/*.json
    Retourne la liste des fichiers créés pour upload.
    """

    # Base URL
    if S3_ENDPOINT:
        base_url = f"{S3_ENDPOINT.rstrip('/')}/{S3_BUCKET}"
    else:
        base_url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com"
    if S3_PREFIX:
        base_url += f"/{S3_PREFIX.strip('/')}"

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
    items_dir = catalog_dir / "items"
    catalog_dir.mkdir(exist_ok=True)
    items_dir.mkdir(exist_ok=True)

    # ── 1. Générer les items STAC ──────────────────────────────────────
    def fmt_date(d):
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}T00:00:00Z"

    output_files = []
    item_links   = []

    for variable in sorted(grouped.keys()):
        meta = var_meta.get(variable, {})
        for version, f in sorted(grouped[variable].items()):
            item_id = f"{variable}_SIM2_{version}"
            item = {
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": item_id,
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-5.2, 41.3], [9.7, 41.3],
                        [9.7, 51.1], [-5.2, 51.1],
                        [-5.2, 41.3]
                    ]]
                },
                "bbox": [-5.2, 41.3, 9.7, 51.1],
                "properties": {
                    "datetime":           None,
                    "start_datetime":     fmt_date(f['date_debut']),
                    "end_datetime":       fmt_date(f['date_fin']),
                    "variable":           variable,
                    "version_type":       version,
                    "description":        safe_str(meta.get('description')),
                    "unite":              safe_str(meta.get('unite')),
                    "periode_agregation": safe_str(meta.get('periode_agregation')),
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
                    {"rel": "root",       "href": f"{base_url}/stac-data/catalog.json",    "type": "application/json"},
                    {"rel": "collection", "href": f"{base_url}/stac-data/collection.json", "type": "application/json"},
                    {"rel": "self",       "href": f"{base_url}/stac-data/items/{item_id}.json", "type": "application/json"}
                ]
            }

            item_path = items_dir / f"{item_id}.json"
            with open(item_path, 'w', encoding='utf-8') as fp:
                json.dump(item, fp, ensure_ascii=False, indent=2)
            output_files.append(item_path)

            item_links.append({
                "rel":   "item",
                "href":  f"{base_url}/stac-data/items/{item_id}.json",
                "type":  "application/json",
                "title": item_id
            })

    # ── 2. Collection ──────────────────────────────────────────────────
    collection = {
        "type":         "Collection",
        "id":           "sim2-safran-isba-modcou",
        "stac_version": "1.0.0",
        "title":        "SIM2 — SAFRAN-ISBA-MODCOU",
        "description":  "Réanalyse hydro-météorologique SIM2 sur la France, grille SAFRAN 8km, données quotidiennes.",
        "license":      "proprietary",
        "extent": {
            "spatial":  {"bbox": [[-5.2, 41.3, 9.7, 51.1]]},
            "temporal": {"interval": [["1958-08-01T00:00:00Z", None]]}
        },
        "links": [
            {"rel": "root", "href": f"{base_url}/stac-data/catalog.json",    "type": "application/json"},
            {"rel": "self", "href": f"{base_url}/stac-data/collection.json", "type": "application/json"},
            *item_links   # ← liens vers chaque item
        ]
    }

    collection_path = catalog_dir / "collection.json"
    with open(collection_path, 'w', encoding='utf-8') as fp:
        json.dump(collection, fp, ensure_ascii=False, indent=2)
    output_files.append(collection_path)

    # # ── 3. Catalog racine ──────────────────────────────────────────────
    # catalog = {
    #     "type":         "Catalog",
    #     "id":           "safran-fairy",
    #     "stac_version": "1.0.0",
    #     "title":        "Catalogue SAFRAN-Fairy",
    #     "description":  "Catalogue des données SIM2 distribuées par SAFRAN-Fairy.",
    #     "links": [
    #         {"rel": "self",  "href": f"{base_url}/stac-data/catalog.json",    "type": "application/json"},
    #         {"rel": "child", "href": f"{base_url}/stac-data/collection.json", "type": "application/json",
    #          "title": "SIM2 SAFRAN-ISBA-MODCOU"}
    #     ]
    # }

    # catalog_path = catalog_dir / "catalog.json"
    # with open(catalog_path, 'w', encoding='utf-8') as fp:
    #     json.dump(catalog, fp, ensure_ascii=False, indent=2)
    # output_files.append(catalog_path)

    print(f"✅ STAC généré : {len(output_files) - 2} items, {len(grouped)} variables")
    # print(f"   → {catalog_dir}/catalog.json")
    print(f"   → {catalog_dir}/collection.json")
    print(f"   → {catalog_dir}/items/ ({len(output_files) - 2} fichiers)")
    return output_files
