#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import sys
from datetime import datetime
from pathlib import Path


def load_query_from_file(path: str) -> str:
    """Le e retorna o conteudo de um arquivo .sql como string unica."""
    if not os.path.isfile(path):
        raise FileNotFoundError(f"O arquivo de query '{path}' nao existe.")
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read().strip()
    if not sql:
        raise ValueError(f"O arquivo '{path}' esta vazio.")
    logging.info("Query carregada de '%s'", path)
    return sql


def setup_logging(log_dir: str, prefix: str) -> Path:
    """Configura logging para arquivo e console."""

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    log_path = Path(log_dir) / f"{prefix}_{ts}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logging.info("Log iniciado em %s", log_path)
    return log_path
