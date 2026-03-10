import os
import requests
import json
import time
from pathlib import Path
from art import tprint
import boto3

from .tools import parse_filename


def get_content_type(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    return {
        '.nc':   'application/x-netcdf',
        '.html': 'text/html; charset=utf-8',
        '.json': 'application/json; charset=utf-8',
    }.get(ext, 'application/octet-stream')


def upload_s3(S3_BUCKET: str,
              S3_PREFIX: str,
              OUTPUT_DIR: str,
              file_paths: list = None,
              overwrite: bool = False,
              organize_by_version: bool = False,
              S3_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY"),
              S3_SECRET_KEY: str = os.getenv("S3_SECRET_KEY"),
              S3_ENDPOINT: str = os.getenv("S3_ENDPOINT"),
              S3_REGION: str = os.getenv("S3_REGION", "eu-west-1")):

    tprint("upload", "small")

    s3 = boto3.client('s3',
                      aws_access_key_id=S3_ACCESS_KEY,
                      aws_secret_access_key=S3_SECRET_KEY,
                      endpoint_url=S3_ENDPOINT,
                      region_name=S3_REGION)
    

    if file_paths is None:
        file_paths = list(Path(OUTPUT_DIR).glob("*.nc"))
    if not file_paths:
        print("\n⚠️  Aucun fichier de données à uploader")
        return []

    print("\nUPLOAD S3")
    print(f"   Bucket: {S3_BUCKET}")
    print(f"   Préfixe: {S3_PREFIX or '(racine)'}")
    print(f"   Fichiers: {len(file_paths)}")

    # Fichiers déjà présents dans le bucket
    existing_keys = set()
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get('Contents', []):
            existing_keys.add(obj['Key'])

    not_uploaded = []
    skipped = []

    for i, file_path in enumerate(file_paths):
        path_obj = Path(file_path)

        if organize_by_version:
            parsed = parse_filename(path_obj.name)
            if parsed:
                version = parsed['version']
                prefix = f"{S3_PREFIX}/{version}".lstrip("/")
            else:
                prefix = S3_PREFIX
        else:
            prefix = S3_PREFIX

        s3_key = f"{prefix}/{path_obj.name}".lstrip("/")

        print(f"\n📤 [{i+1}/{len(file_paths)}] {path_obj.name}")
        print(f"   → Clé S3: {s3_key}")

        if s3_key in existing_keys and not overwrite:
            print(f"   ⏭️  Fichier déjà présent, ignoré")
            skipped.append(file_path)
            continue

        try:
            start_time = time.time()
            file_size = os.path.getsize(file_path) / (1024**2)

            s3.upload_file(
                str(file_path),
                S3_BUCKET,
                s3_key,
                ExtraArgs={'ContentType': get_content_type(path_obj.name)}
            )

            elapsed = time.time() - start_time
            speed = file_size / elapsed
            print(f"   ✅ Upload: {round(file_size, 2)} MB en {round(elapsed, 2)}s @ {round(speed, 2)} MB/s")

            # URL stable du fichier
            url = f"{S3_ENDPOINT.rstrip('/')}/{S3_BUCKET}/{s3_key}"
            print(f"   🔗 URL: {url}")

        except Exception as e:
            not_uploaded.append(file_path)
            print(f"   ❌ Erreur: {str(e)}")

    print("\nRÉSUMÉ")
    uploaded_count = len(file_paths) - len(not_uploaded) - len(skipped)
    print(f"   - {uploaded_count}/{len(file_paths)} fichier(s) uploadés")
    if skipped:
        print(f"   - ⏭️  {len(skipped)} fichier(s) ignorés")
    if not_uploaded:
        print(f"   - ⚠️  {len(not_uploaded)} échec(s)")

    return not_uploaded
