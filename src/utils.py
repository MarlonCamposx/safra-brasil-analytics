"""
utils.py
--------
Funções utilitárias reutilizadas pelos scripts de ingestão.
"""

import logging

import pandas as pd
import requests
from google.cloud import bigquery


def get_logger(name: str) -> logging.Logger:
    """Retorna um logger configurado com formato padronizado.

    Args:
        name: Nome do logger, tipicamente __name__ do módulo chamador.

    Returns:
        Logger configurado com nível INFO e formato '%(levelname)s %(message)s'.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    return logging.getLogger(name)


def http_get_json(url: str, headers: dict | None = None, timeout: int = 60) -> list | dict:
    """Executa uma requisição GET e retorna o corpo como JSON.

    Args:
        url: URL completa do endpoint.
        headers: Headers HTTP opcionais.
        timeout: Tempo máximo de espera em segundos (padrão: 60).

    Returns:
        JSON deserializado como list ou dict.

    Raises:
        requests.HTTPError: Se a resposta HTTP não for 2xx.
    """
    response = requests.get(url, headers=headers or {}, timeout=timeout)
    response.raise_for_status()
    return response.json()


def upload_to_bigquery(df: pd.DataFrame, table_id: str) -> None:
    """Carrega um DataFrame na tabela BigQuery, substituindo o conteúdo existente.

    Args:
        df: DataFrame com os dados a serem carregados.
        table_id: Identificador completo da tabela no formato 'projeto.dataset.tabela'.

    Returns:
        None
    """
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    client = bigquery.Client()
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
