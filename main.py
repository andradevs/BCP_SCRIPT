#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import subprocess
import sys
import gzip
import shutil
import logging
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

from utils import load_query_from_file, setup_logging

def run_bcp_export(
    server: str,
    database: str,
    username: str,
    password: str,
    query: str,
    output_file: str,
    bcp_path: str = "bcp",
    field_terminator: str = ";",
) -> None:
    """Executa o bcp exportando o resultado da query para um arquivo."""

    cmd = [
        bcp_path,
        query,
        "queryout",
        output_file,
        "-S",
        server,
        "-d",
        database,
        "-U",
        username,
        "-P",
        password,
        "-c",
        "-t",
        field_terminator,
    ]

    logging.info("Executando BCP: %s", " ".join(cmd))

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Erro no BCP {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )

    logging.info("BCP concluido. Arquivo gerado: %s", output_file)


def compress_gzip(source_path: str) -> str:
    """Compacta o arquivo em gzip e retorna o caminho do .gz."""

    gz_path = f"{source_path}.gz"
    logging.info("Compactando '%s' para '%s' (gzip)", source_path, gz_path)
    with open(source_path, "rb") as src, gzip.open(gz_path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    return gz_path


def upload_to_s3(
    file_path: str,
    access_key_id: str,
    secret_access_key: str,
    region: str,
    bucket: str,
    key: str,
) -> None:
    """Envia o arquivo gerado para o S3."""

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Arquivo '{file_path}' nao encontrado.")

    logging.info("Enviando '%s' para s3://%s/%s", file_path, bucket, key)

    session = boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region,
    )

    s3 = session.client("s3")

    try:
        s3.upload_file(file_path, bucket, key)
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Erro ao enviar para o S3: {e}")

    logging.info("Upload concluido com sucesso.")


def main():
    parser = argparse.ArgumentParser(
        description="Exporta queries .sql via BCP e envia o .bcp.gz ao S3."
    )
    parser.add_argument(
        "--scripts",
        nargs="+",
        help="Lista de arquivos .sql a executar (nomes ou caminhos); se omitido, roda todos do diretorio.",
    )
    args = parser.parse_args()

    load_dotenv()

    scripts_dir = os.getenv("SCRIPTS_DIR", os.path.join(os.getcwd(), "scripts"))
    output_dir = os.getenv("OUTPUT_DIR", os.getcwd())
    log_dir = os.getenv("LOG_DIR", os.path.join(os.getcwd(), "logs"))

    setup_logging(log_dir, "export")

    # parametros SQL
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")

    # parametros gerais
    bcp_path = os.getenv("BCP_PATH", "bcp")
    field_terminator = os.getenv("FIELD_TERMINATOR", ";")

    # S3
    s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
    s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")
    s3_region = os.getenv("S3_REGION")
    s3_bucket = os.getenv("S3_BUCKET")
    s3_key = os.getenv("S3_KEY")

    # validacao
    required = [
        ("SERVER", server),
        ("DATABASE", database),
        ("USERNAME", username),
        ("PASSWORD", password),
        ("S3_ACCESS_KEY_ID", s3_access_key_id),
        ("S3_SECRET_ACCESS_KEY", s3_secret_access_key),
        ("S3_REGION", s3_region),
        ("S3_BUCKET", s3_bucket),
        ("S3_KEY", s3_key),
    ]

    missing = [name for name, val in required if not val]

    if missing:
        raise RuntimeError(
            f"As seguintes variaveis nao foram definidas no .env: {', '.join(missing)}"
        )

    if not os.path.isdir(scripts_dir):
        raise RuntimeError(f"O diretorio de scripts nao existe: {scripts_dir}")

    all_sql_files = sorted(Path(scripts_dir).glob("*.sql"))
    if not all_sql_files:
        raise RuntimeError(f"Nenhum arquivo .sql encontrado em {scripts_dir}")

    if args.scripts:
        requested = set()
        for entry in args.scripts:
            name = os.path.basename(entry)
            if not name.lower().endswith(".sql"):
                name = f"{name}.sql"
            requested.add(name)

        selected = [p for p in all_sql_files if p.name in requested]
        missing = requested - {p.name for p in selected}
        if missing:
            raise RuntimeError(
                f"Arquivos solicitados nao encontrados em {scripts_dir}: {', '.join(sorted(missing))}"
            )
        sql_files = selected
    else:
        sql_files = all_sql_files

    os.makedirs(output_dir, exist_ok=True)

    failures = []
    for sql_path in sql_files:
        try:
            query = load_query_from_file(str(sql_path))
            output_file = os.path.join(output_dir, f"{sql_path.stem}.bcp")
            logging.info(
                "Executando query de '%s' com output '%s'",
                sql_path.name,
                output_file,
            )

            run_bcp_export(
                server=server,
                database=database,
                username=username,
                password=password,
                query=query,
                output_file=output_file,
                bcp_path=bcp_path,
                field_terminator=field_terminator,
            )

            compressed_file = compress_gzip(output_file)
            s3_object_key = f"{s3_key.rstrip('/')}/{os.path.basename(compressed_file)}"

            upload_to_s3(
                file_path=compressed_file,
                access_key_id=s3_access_key_id,
                secret_access_key=s3_secret_access_key,
                region=s3_region,
                bucket=s3_bucket,
                key=s3_object_key,
            )
        except Exception as e:
            failures.append((sql_path.name, str(e)))
            logging.error("Falha ao processar %s: %s", sql_path.name, e)
            continue

    if failures:
        logging.error("Concluido com falhas em %d script(s).", len(failures))
        for name, err in failures:
            logging.error(" - %s: %s", name, err)
        sys.exit(1)


if __name__ == "__main__":
    main()
