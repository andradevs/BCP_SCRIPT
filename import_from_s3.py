#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import subprocess
import sys
import gzip
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv


def str_to_bool(value: str, default: bool = False) -> bool:
    """Converte strings comuns de verdade/falso em booleano."""

    truthy = {"1", "true", "t", "yes", "y", "sim"}
    falsy = {"0", "false", "f", "no", "n", "nao", "não"}

    if value is None:
        return default

    lower = value.strip().lower()
    if lower in truthy:
        return True
    if lower in falsy:
        return False
    return default


def infer_table_name_from_file(filename: str) -> str:
    """Retorna o nome da tabela a partir do nome do arquivo .bcp."""

    name = Path(filename).name
    if name.endswith(".gz"):
        name = name[: -len(".gz")]
    stem = Path(name).stem
    match = re.match(r"(.+)_\d{8}_\d{6}$", stem)
    return match.group(1) if match else stem


def parse_table_name(raw_table: str) -> tuple[str, str]:
    """Retorna schema e tabela a partir de um identificador bruto."""

    cleaned = raw_table.strip().strip("[]")

    if "." in cleaned:
        schema, table = cleaned.split(".", 1)
    else:
        schema, table = "dbo", cleaned

    return schema, table


def normalize_table_identifiers(raw_table: str) -> tuple[str, str]:
    """
    Normaliza o nome da tabela para uso no TRUNCATE (com colchetes)
    e no bcp (schema.nome).
    """

    schema, table = parse_table_name(raw_table)

    bracketed = f"[{schema}].[{table}]"
    bcp_name = f"{schema}.{table}"
    return bracketed, bcp_name


def ensure_staging_table(
    sqlcmd_path: str,
    server: str,
    database: str,
    username: str,
    password: str,
    base_table_for_sql: str,
    staging_table_for_sql: str,
) -> None:
    """Cria a tabela de staging se ela ainda nao existir."""

    query = (
        f"IF OBJECT_ID(N'{base_table_for_sql}', 'U') IS NULL\n"
        "BEGIN\n"
        "    THROW 50001, 'Tabela base nao encontrada no banco de destino.', 1;\n"
        "END\n"
        f"IF OBJECT_ID(N'{staging_table_for_sql}', 'U') IS NULL\n"
        "BEGIN\n"
        "    DECLARE @sql NVARCHAR(MAX) = N'CREATE TABLE "
        f"{staging_table_for_sql} (' +\n"
        "        STUFF((\n"
        "            SELECT\n"
        "                ',[' + c.name + '] ' +\n"
        "                t.name +\n"
        "                CASE\n"
        "                    WHEN t.name IN ('varchar','char','varbinary','binary')\n"
        "                        THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' "
        "ELSE CAST(c.max_length AS NVARCHAR(10)) END + ')'\n"
        "                    WHEN t.name IN ('nvarchar','nchar')\n"
        "                        THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' "
        "ELSE CAST(c.max_length / 2 AS NVARCHAR(10)) END + ')'\n"
        "                    WHEN t.name IN ('decimal','numeric')\n"
        "                        THEN '(' + CAST(c.precision AS NVARCHAR(10)) + ',' + "
        "CAST(c.scale AS NVARCHAR(10)) + ')'\n"
        "                    WHEN t.name IN ('datetime2','time','datetimeoffset')\n"
        "                        THEN '(' + CAST(c.scale AS NVARCHAR(10)) + ')'\n"
        "                    ELSE ''\n"
        "                END +\n"
        "                CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END\n"
        "            FROM sys.columns c\n"
        "            JOIN sys.types t ON c.user_type_id = t.user_type_id\n"
        f"            WHERE c.object_id = OBJECT_ID(N'{base_table_for_sql}')\n"
        "            ORDER BY c.column_id\n"
        "            FOR XML PATH(''), TYPE\n"
        "        ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') +\n"
        "        ')';\n"
        "    EXEC sp_executesql @sql;\n"
        "END"
    )
    cmd = [
        sqlcmd_path,
        "-S",
        server,
        "-d",
        database,
        "-U",
        username,
        "-P",
        password,
        "-Q",
        query,
    ]

    logging.info("Garantindo tabela staging: %s", " ".join(cmd))

    result = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Erro ao criar tabela staging ({result.returncode}). "
            f"STDERR: {result.stderr}"
        )


def resolve_object_key(
    s3_client,
    bucket: str,
    prefix: str,
    explicit_key: Optional[str],
) -> str:
    """Retorna a chave do objeto .bcp a ser baixado."""

    if explicit_key:
        return explicit_key

    latest_key = None
    latest_modified = None

    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key_lower = obj["Key"].lower()
            if not (key_lower.endswith(".bcp") or key_lower.endswith(".bcp.gz")):
                continue
            if latest_modified is None or obj["LastModified"] > latest_modified:
                latest_key = obj["Key"]
                latest_modified = obj["LastModified"]

    if not latest_key:
        raise RuntimeError(
            f"Nenhum arquivo .bcp encontrado em s3://{bucket}/{prefix}"
        )

    return latest_key


def download_bcp(
    s3_client,
    bucket: str,
    key: str,
    download_dir: str,
) -> Path:
    """Baixa o arquivo .bcp e retorna o caminho local."""

    Path(download_dir).mkdir(parents=True, exist_ok=True)
    destination = Path(download_dir) / Path(key).name

    try:
        logging.info("Baixando s3://%s/%s para %s", bucket, key, destination)
        s3_client.download_file(bucket, key, str(destination))
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Erro ao baixar do S3: {e}")

    return destination


def maybe_decompress_gzip(path: Path) -> Path:
    """Se o arquivo for .gz, descompacta e retorna o caminho descompactado."""

    if path.suffix != ".gz":
        return path

    target = path.with_suffix("")  # remove .gz
    logging.info("Descompactando '%s' para '%s'", path, target)
    with gzip.open(path, "rb") as src, open(target, "wb") as dst:
        # Copia em streaming para evitar carregar arquivos grandes em memoria
        shutil.copyfileobj(src, dst)
    return target


def truncate_table(
    sqlcmd_path: str,
    server: str,
    database: str,
    username: str,
    password: str,
    table_for_sql: str,
) -> None:
    """Executa TRUNCATE TABLE usando sqlcmd."""

    cmd = [
        sqlcmd_path,
        "-S",
        server,
        "-d",
        database,
        "-U",
        username,
        "-P",
        password,
        "-Q",
        f"TRUNCATE TABLE {table_for_sql}",
    ]

    logging.info("Truncando tabela: %s", " ".join(cmd))

    result = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Erro ao truncar tabela ({result.returncode}). "
            f"STDERR: {result.stderr}"
        )


def confirm_truncate(table_for_sql: str) -> None:
    """Solicita confirmacao antes de truncar a tabela."""

    answer = input(
        f"Confirmar TRUNCATE em {table_for_sql}? "
        "Digite SIM para continuar: "
    ).strip()

    if answer.upper() != "SIM":
        raise RuntimeError("Operacao cancelada pelo usuario.")


def run_bcp_import(
    bcp_path: str,
    table_for_bcp: str,
    server: str,
    database: str,
    username: str,
    password: str,
    file_path: str,
    field_terminator: str,
    keep_identity: bool,
    max_errors: int,
    error_file: Optional[str] = None,
) -> None:
    """Importa os dados do arquivo .bcp para a tabela alvo."""

    cmd = [
        bcp_path,
        table_for_bcp,
        "in",
        file_path,
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

    if keep_identity:
        cmd.append("-E")

    if max_errors is not None:
        cmd.extend(["-m", str(max_errors)])

    if error_file:
        Path(error_file).parent.mkdir(parents=True, exist_ok=True)
        cmd.extend(["-e", error_file])

    logging.info("Importando via BCP: %s", " ".join(cmd))

    result = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    if result.returncode != 0:
        example_rows = ""
        if error_file:
            try:
                lines = []
                with open(error_file, "r", encoding="utf-8", errors="replace") as f:
                    for _ in range(5):
                        line = f.readline()
                        if not line:
                            break
                        lines.append(line)
                if lines:
                    example_rows = (
                        "\n\nExemplo de linhas com erro (arquivo %s):\n%s"
                        % (error_file, "".join(lines))
                    )
            except OSError:
                pass

        raise RuntimeError(
            f"Erro no BCP import ({result.returncode})\n"
            f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
            f"{example_rows}"
        )


def main():
    load_dotenv()

    # DB
    server = os.getenv("STAGE_DB_SERVER")
    database = os.getenv("STAGE_DB_DATABASE")
    username = os.getenv("STAGE_DB_USERNAME")
    password = os.getenv("STAGE_DB_PASSWORD")

    # S3
    s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
    s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")
    s3_region = os.getenv("S3_REGION")
    s3_bucket = os.getenv("S3_BUCKET")
    s3_prefix = os.getenv("S3_KEY", "")
    s3_object_key = os.getenv("S3_OBJECT_KEY")  # opcional: chave especifica

    # Ferramentas
    bcp_path = os.getenv("BCP_PATH", "bcp")
    sqlcmd_path = os.getenv("SQLCMD_PATH", "sqlcmd")
    field_terminator = os.getenv("FIELD_TERMINATOR", ";")
    download_dir = os.getenv(
        "DOWNLOAD_DIR", os.path.join(os.getcwd(), "downloads")
    )
    log_dir = os.getenv("LOG_DIR", os.path.join(os.getcwd(), "logs"))
    keep_identity = str_to_bool(os.getenv("BCP_KEEP_IDENTITY"), default=True)
    max_errors_env = os.getenv("BCP_MAX_ERRORS")
    error_file_env = os.getenv("BCP_ERROR_FILE")

    try:
        max_errors = int(max_errors_env) if max_errors_env is not None else 1
        if max_errors < 0:
            raise ValueError("BCP_MAX_ERRORS nao pode ser negativo")
    except ValueError:
        raise RuntimeError(
            "BCP_MAX_ERRORS deve ser um numero inteiro maior ou igual a zero"
        )

    ts = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    error_file_path = error_file_env or str(
        Path(log_dir) / f"import_errors_{ts}.err"
    )

    required = [
        ("STAGE_DB_SERVER", server),
        ("STAGE_DB_DATABASE", database),
        ("STAGE_DB_USERNAME", username),
        ("STAGE_DB_PASSWORD", password),
        ("S3_ACCESS_KEY_ID", s3_access_key_id),
        ("S3_SECRET_ACCESS_KEY", s3_secret_access_key),
        ("S3_REGION", s3_region),
        ("S3_BUCKET", s3_bucket),
    ]

    missing = [name for name, val in required if not val]
    if missing:
        raise RuntimeError(
            f"As seguintes variaveis nao foram definidas: {', '.join(missing)}"
        )

    if not s3_object_key and not s3_prefix:
        raise RuntimeError(
            "Defina S3_OBJECT_KEY ou pelo menos S3_KEY para listar o prefixo."
        )

    session = boto3.Session(
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
        region_name=s3_region,
    )
    s3_client = session.client("s3")

    # logging
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_path = Path(log_dir) / f"import_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Log iniciado em %s", log_path)
    logging.info("BCP_KEEP_IDENTITY=%s", keep_identity)
    logging.info("BCP_MAX_ERRORS=%s", max_errors)
    logging.info("BCP_ERROR_FILE=%s", error_file_path)

    try:
        object_key = resolve_object_key(
            s3_client=s3_client,
            bucket=s3_bucket,
            prefix=s3_prefix,
            explicit_key=s3_object_key,
        )
        logging.info("Arquivo selecionado: s3://%s/%s", s3_bucket, object_key)

        local_file = download_bcp(
            s3_client=s3_client,
            bucket=s3_bucket,
            key=object_key,
            download_dir=download_dir,
        )
        local_file = maybe_decompress_gzip(local_file)

        raw_table = infer_table_name_from_file(local_file.name)
        schema, table = parse_table_name(raw_table)
        staging_raw_table = f"{schema}.{table}_STAGING"
        table_for_sql, table_for_bcp = normalize_table_identifiers(
            staging_raw_table
        )
        base_table_for_sql, _ = normalize_table_identifiers(raw_table)
        logging.info(
            "Tabela inferida: %s (bcp: %s) a partir de %s",
            table_for_sql,
            table_for_bcp,
            local_file.name,
        )

        ensure_staging_table(
            sqlcmd_path=sqlcmd_path,
            server=server,
            database=database,
            username=username,
            password=password,
            base_table_for_sql=base_table_for_sql,
            staging_table_for_sql=table_for_sql,
        )

        confirm_truncate(table_for_sql)

        truncate_table(
            sqlcmd_path=sqlcmd_path,
            server=server,
            database=database,
            username=username,
            password=password,
            table_for_sql=table_for_sql,
        )

        run_bcp_import(
            bcp_path=bcp_path,
            table_for_bcp=table_for_bcp,
            server=server,
            database=database,
            username=username,
            password=password,
            file_path=str(local_file),
            field_terminator=field_terminator,
            keep_identity=keep_identity,
            max_errors=max_errors,
            error_file=error_file_path,
        )

        logging.info(
            "Importacao concluida com sucesso na tabela %s", table_for_sql
        )

    except Exception as e:
        logging.error("ERRO: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
