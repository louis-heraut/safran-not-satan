import os
import requests
import json
import time
import pandas as pd
from pathlib import Path
from art import tprint

from .dataverse_tools import delete_file_by_name


def generate_index(OUTPUT_DIR: str,
                   S3_BUCKET: str,
                   S3_PREFIX: str = "",
                   METADATA_VARIABLES_FILE: str = None,
                   INDEX_PATH: str = "index.html",
                   S3_ENDPOINT: str = os.getenv("S3_ENDPOINT"),
                   S3_REGION: str = os.getenv("S3_REGION", "eu-west-1")):
    """
    Génère un fichier HTML listant les liens S3 groupés par variable et type.
    Les URLs sont construites depuis les noms de fichiers locaux.
    """

    file_paths = list(Path(OUTPUT_DIR).glob("*.nc"))
    if not file_paths:
        print("\n⚠️  Aucun fichier à uploader")
        return []
    
    # Base URL S3
    if S3_ENDPOINT:
        base_url = f"{S3_ENDPOINT.rstrip('/')}/{S3_BUCKET}"
    else:
        base_url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com"
    if S3_PREFIX:
        base_url += f"/{S3_PREFIX.strip('/')}"
    
    # Charger les descriptions depuis le CSV
    var_meta = {}
    if METADATA_VARIABLES_FILE and Path(METADATA_VARIABLES_FILE).exists():
        df = pd.read_csv(METADATA_VARIABLES_FILE, index_col='variable')
        var_meta = df.to_dict(orient='index')
        
    from collections import defaultdict
    grouped = defaultdict(lambda: defaultdict(list))

    type_order = ['latest', 'previous', 'historical']

    for file_path in file_paths:
        name = Path(file_path).name
        # Extraire variable et type depuis le nom
        parts = name.split('_QUOT_SIM2_')
        if len(parts) != 2:
            continue
        variable = parts[0]
        type_and_dates = parts[1]

        file_type = type_and_dates.split('-')[0]  # latest / previous / historical
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

        # Infos métadonnées si disponibles
        if description:
            lines.append(f'<p><strong>{description}</strong>')
            details = []
            if unite:
                details.append(f'Unité : {unite}')
            if periode:
                details.append(f'Période d\'agrégation : {periode}')
            if details:
                lines.append(f'<br>{"  —  ".join(details)}')
            lines.append('</p>')

        # Liens par type
        types = grouped[variable]
        for file_type in type_order:
            if file_type not in types:
                continue
            files = types[file_type]

            label = {
                'latest': 'Latest — update journalière',
                'previous': 'Previous — update mensuelle',
                'historical': 'Historical — update décénale'
            }.get(file_type, file_type)

            lines.append(f'<p><strong>{label}</strong><br>')
            for name, url in sorted(files):
                lines.append(f'<a href="{url}">{name}</a><br>')
            lines.append('</p>')

        lines.append('<hr>')

    html = '\n'.join(lines)

    with open(INDEX_PATH, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"✅ HTML généré : {INDEX_PATH} ({len(grouped)} variables)")
    return INDEX_PATH


def upload_dataverse_index(INDEX_PATH: str,
                           RDG_DATASET_DOI: str,
                           RDG_BASE_URL: str = os.getenv("RDG_BASE_URL"),
                           RDG_API_TOKEN: str = os.getenv("RDG_API_TOKEN")):
    """Upload (ou remplace) le fichier index HTML dans un dataset Dataverse."""

    path_obj = Path(INDEX_PATH)
    headers = {'X-Dataverse-key': RDG_API_TOKEN}

    print(f"\n📤 Upload index Dataverse: {path_obj.name}")

    # Supprimer l'ancien si présent
    delete_file_by_name(RDG_DATASET_DOI, path_obj.name, RDG_BASE_URL, RDG_API_TOKEN)

    # Upload
    url = f"{RDG_BASE_URL}/api/datasets/:persistentId/add?persistentId={RDG_DATASET_DOI}"
    json_data = {"description": "Index des fichiers disponibles sur S3", 
                 "restrict": "false", 
                 "tabIngest": "false"}  # important : pas d'ingest pour un HTML

    with open(INDEX_PATH, 'rb') as f:
        response = requests.post(url, headers=headers, files={
            'file': (path_obj.name, f, 'text/html'),
            'jsonData': (None, json.dumps(json_data), 'application/json')
        })

    if response.status_code in [200, 201]:
        print(f"   ✅ Index uploadé")
    else:
        print(f"   ❌ Échec: {response.status_code} - {response.text}")



def update_dataverse_index(OUTPUT_DIR: str,
                           S3_BUCKET: str,
                           S3_PREFIX: str = "",
                           METADATA_VARIABLES_FILE: str = None,
                           INDEX_PATH: str = "index.html",
                           S3_ENDPOINT: str = os.getenv("S3_ENDPOINT"),
                           S3_REGION: str = os.getenv("S3_REGION", "eu-west-1"),
                           RDG_DATASET_DOI: str = os.getenv("RDG_DATASET_DOI"),
                           RDG_BASE_URL: str = os.getenv("RDG_BASE_URL"),
                           RDG_API_TOKEN: str = os.getenv("RDG_API_TOKEN")):
    
    generate_index(OUTPUT_DIR=OUTPUT_DIR,
                   S3_BUCKET=S3_BUCKET,
                   S3_PREFIX=S3_PREFIX,
                   METADATA_VARIABLES_FILE=METADATA_VARIABLES_FILE,
                   INDEX_PATH=INDEX_PATH,
                   S3_ENDPOINT=S3_ENDPOINT,
                   S3_REGION=S3_REGION)
    
    upload_dataverse_index(INDEX_PATH=INDEX_PATH,
                           RDG_DATASET_DOI=RDG_DATASET_DOI,
                           RDG_BASE_URL=RDG_BASE_URL,
                           RDG_API_TOKEN=RDG_API_TOKEN)
