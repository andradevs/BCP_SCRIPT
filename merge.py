#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

from utils import load_query_from_file, setup_logging


def run_sqlcmd(
    sqlcmd_path: str,
    server: str,
    database: str,
    username: str,
    password: str,
    script_path: str,
) -> None:
    """Executa um script .sql com sqlcmd."""

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
        "-i",
        script_path,
    ]

    logging.info("Executando script via sqlcmd: %s", " ".join(cmd))

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Erro no sqlcmd {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Executa scripts de merge conectando em um unico banco."
    )
    parser.add_argument(
        "--scripts",
        nargs="+",
        help="Lista de arquivos .sql a executar (nomes ou caminhos); se omitido, roda todos do diretorio.",
    )
    args = parser.parse_args()

    load_dotenv()

    scripts_dir = os.getenv(
        "SCRIPTS_MERGE_DIR", os.path.join(os.getcwd(), "scripts_merge")
    )
    log_dir = os.getenv("LOG_DIR", os.path.join(os.getcwd(), "logs"))

    setup_logging(log_dir, "merge")

    server = os.getenv("DEST_DB_SERVER")
    database = os.getenv("DEST_DB_DATABASE")
    username = os.getenv("DEST_DB_USERNAME")
    password = os.getenv("DEST_DB_PASSWORD")

    sqlcmd_path = os.getenv("SQLCMD_PATH", "sqlcmd")

    required = [
        ("DEST_DB_SERVER", server),
        ("DEST_DB_DATABASE", database),
        ("DEST_DB_USERNAME", username),
        ("DEST_DB_PASSWORD", password),
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

    failures = []
    for sql_path in sql_files:
        try:
            load_query_from_file(str(sql_path))
            logging.info(
                "Executando %s no banco de destino configurado.", sql_path.name
            )
            run_sqlcmd(
                sqlcmd_path=sqlcmd_path,
                server=server,
                database=database,
                username=username,
                password=password,
                script_path=str(sql_path),
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
