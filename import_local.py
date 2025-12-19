#!/usr/bin/env python
# -*- coding: utf-8 -*-

import gzip
import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import shutil

from dotenv import load_dotenv


def str_to_bool(value: str, default: bool = False) -> bool:
    """Converte strings comuns de verdade/falso em booleano."""

    truthy = {"1", "true", "t", "yes", "y", "sim"}
    falsy = {"0", "false", "f", "no", "n", "nao"}

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
        f"IF OBJECT_ID(N'{staging_table_for_sql}', 'U') IS NULL\n"
        "BEGIN\n"
        f"    SELECT TOP 0 * INTO {staging_table_for_sql} "
        f"FROM {base_table_for_sql};\n"
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


def resolve_local_bcp(local_dir: str, explicit_file: Optional[str]) -> Path:
    """Seleciona o arquivo .bcp/.bcp.gz a ser importado no modo local."""

    directory = Path(local_dir)

    if explicit_file:
        candidate = Path(explicit_file)
        if not candidate.is_absolute():
            candidate = directory / candidate
        if not candidate.is_file():
            raise RuntimeError(f"Arquivo local nao encontrado: {candidate}")
        return candidate

    if not directory.is_dir():
        raise RuntimeError(
            f"Diretorio de importacao local nao encontrado: {directory}"
        )

    candidates = list(directory.glob("*.bcp")) + list(directory.glob("*.bcp.gz"))
    if not candidates:
        raise RuntimeError(
            f"Nenhum arquivo .bcp encontrado em {directory}. "
            "Coloque o arquivo na pasta e tente novamente."
        )

    # Usa o arquivo mais recente para evitar escolher um dump antigo.
    return max(candidates, key=lambda p: p.stat().st_mtime)


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

    # Importacao local
    local_import_dir = os.getenv(
        "LOCAL_IMPORT_DIR", os.path.join(os.getcwd(), "Subida")
    )
    local_import_file = os.getenv("LOCAL_IMPORT_FILE")

    # Ferramentas
    bcp_path = os.getenv("BCP_PATH", "bcp")
    sqlcmd_path = os.getenv("SQLCMD_PATH", "sqlcmd")
    field_terminator = os.getenv("FIELD_TERMINATOR", ";")
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
    ]

    missing = [name for name, val in required if not val]
    if missing:
        raise RuntimeError(
            f"As seguintes variaveis nao foram definidas: {', '.join(missing)}"
        )

    # logging
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_path = Path(log_dir) / f"import_local_{ts}.log"
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
        local_file = resolve_local_bcp(
            local_dir=local_import_dir, explicit_file=local_import_file
        )
        logging.info("Arquivo selecionado (local): %s", local_file)

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
            "Importacao local concluida com sucesso na tabela %s", table_for_sql
        )

    except Exception as e:
        logging.error("ERRO: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
