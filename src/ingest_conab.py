"""
ingest_conab.py
---------------
Fonte      : CONAB — Séries Históricas de Safras (grãos)
Frequência : Anual (atualizado a cada levantamento)
Destino    : BigQuery raw.raw_crop_harvest
Execução   : python -m src.ingest_conab

Baixa os XLS públicos da CONAB (Soja e Milho Total), converte de formato
wide para long e carrega os dados brutos na tabela raw.raw_crop_harvest.
"""

import io
import logging
from pathlib import Path

import pandas as pd
import requests
from google.cloud import bigquery

from config.settings import DATA_RAW_PATH, PROJECT_ID

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_BASE_URL = (
    "https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias"
    "/safras/series-historicas/graos"
)

_CROP_URLS = {
    "Soja": f"{_BASE_URL}/soja/sojaseriehist.xls/@@download/file",
    "Milho": f"{_BASE_URL}/milho/milhototalseriehist.xls/@@download/file",
}

# sheet index → target column name
_SHEET_METRICS = {
    0: "area_plantada_mil_ha",
    1: "produtividade_kg_ha",
    2: "producao_mil_ton",
}

_VALID_UF_CODES = {
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

TABLE_ID = f"{PROJECT_ID}.raw.raw_crop_harvest"

# Rows 0-4 are metadata; row 5 is the actual column header
_CONAB_SKIP_ROWS = 5

_STATE_NAME_TO_CODE = {
    "acre": "AC",
    "alagoas": "AL",
    "amapá": "AP",
    "amazonas": "AM",
    "bahia": "BA",
    "ceará": "CE",
    "distrito federal": "DF",
    "espírito santo": "ES",
    "goiás": "GO",
    "maranhão": "MA",
    "mato grosso": "MT",
    "mato grosso do sul": "MS",
    "minas gerais": "MG",
    "pará": "PA",
    "paraíba": "PB",
    "paraná": "PR",
    "pernambuco": "PE",
    "piauí": "PI",
    "rio de janeiro": "RJ",
    "rio grande do norte": "RN",
    "rio grande do sul": "RS",
    "rondônia": "RO",
    "roraima": "RR",
    "santa catarina": "SC",
    "são paulo": "SP",
    "sergipe": "SE",
    "tocantins": "TO",
}


def normalize_state_code(state) -> str:
    """Normaliza um valor de UF para sigla de 2 letras maiúsculas.

    Aceita siglas (ex: 'sp', '  MT ') e nomes completos (ex: 'Mato Grosso').
    Levanta ValueError para None ou string vazia.
    """
    if state is None:
        raise ValueError("state code cannot be None")
    cleaned = str(state).strip()
    if not cleaned:
        raise ValueError("state code cannot be empty")
    upper = cleaned.upper()
    if len(upper) == 2:
        return upper
    mapped = _STATE_NAME_TO_CODE.get(cleaned.lower())
    if mapped:
        return mapped
    return upper


def _transform_sheet(df_raw: pd.DataFrame, metric_col: str) -> pd.DataFrame:
    """Converte uma aba wide (anos como colunas) para long (uf, safra, métrica).

    Filtra apenas linhas de UF válidas (siglas de 2 letras).
    Extrai o ano safra no formato 'AAAA/AA' descartando sufixos de previsão.
    """
    uf_col = df_raw.columns[0]
    df = df_raw.rename(columns={uf_col: "uf"})
    df = df.dropna(subset=["uf"])
    df["uf"] = df["uf"].astype(str).str.strip().str.upper()
    df = df[df["uf"].isin(_VALID_UF_CODES)].copy()
    year_cols = [c for c in df.columns if c != "uf"]
    df = df.melt(id_vars=["uf"], value_vars=year_cols, var_name="safra", value_name=metric_col)
    df["safra"] = df["safra"].astype(str).str.extract(r"(\d{4}/\d{2,4})", expand=False)
    df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce")
    return df.dropna(subset=["safra"]).reset_index(drop=True)


def parse_crop_content(content: bytes, crop_name: str) -> pd.DataFrame:
    """Lê o conteúdo bruto de um XLS da CONAB e retorna DataFrame long para um produto."""
    sheet_dfs = []
    for sheet_idx, metric_col in _SHEET_METRICS.items():
        raw = pd.read_excel(
            io.BytesIO(content),
            engine="xlrd",
            sheet_name=sheet_idx,
            skiprows=_CONAB_SKIP_ROWS,
            header=0,
            dtype=str,
        )
        sheet_dfs.append(_transform_sheet(raw, metric_col))

    df = sheet_dfs[0]
    for other in sheet_dfs[1:]:
        df = df.merge(other, on=["uf", "safra"], how="outer")

    df["produto"] = crop_name
    cols = [
        "produto",
        "safra",
        "uf",
        "area_plantada_mil_ha",
        "produtividade_kg_ha",
        "producao_mil_ton",
    ]
    return df[cols].reset_index(drop=True)


def download_conab_xls(crop_name: str, url: str, dest_dir: str = DATA_RAW_PATH) -> Path:
    """Baixa e armazena em cache o XLS de um produto. Retorna o Path do arquivo."""
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    file_path = dest / f"conab_{crop_name.lower()}_serie_historica.xls"
    if file_path.exists():
        logger.info("XLS já existe em %s, pulando download.", file_path)
        return file_path
    logger.info("Baixando dados de %s da CONAB...", crop_name)
    response = requests.get(url, headers=_HEADERS, timeout=60)
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "")
    if "html" in content_type or response.content[:5] == b"<!DOC":
        raise RuntimeError(
            f"Servidor retornou HTML para {crop_name}. "
            f"Content-Type: {content_type!r}. URL: {url}"
        )
    file_path.write_bytes(response.content)
    logger.info("Arquivo salvo em %s (%d bytes).", file_path, len(response.content))
    return file_path


def load_to_bigquery(df: pd.DataFrame, table_id: str) -> None:
    """Carrega o DataFrame na tabela BigQuery especificada via load_table_from_dataframe."""
    client = bigquery.Client()
    client.load_table_from_dataframe(df, table_id)


def main() -> None:
    all_dfs = []
    for crop_name, url in _CROP_URLS.items():
        file_path = download_conab_xls(crop_name, url)
        logger.info("Processando %s...", crop_name)
        df = parse_crop_content(file_path.read_bytes(), crop_name)
        logger.info("%s: %d linhas.", crop_name, len(df))
        all_dfs.append(df)

    df_combined = pd.concat(all_dfs, ignore_index=True)
    logger.info("Total de linhas: %d", len(df_combined))
    load_to_bigquery(df_combined, TABLE_ID)
    logger.info("Carga concluída em %s.", TABLE_ID)


if __name__ == "__main__":
    main()
