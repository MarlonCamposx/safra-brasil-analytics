"""
ingest_sidra.py
---------------
Fonte      : IBGE — API SIDRA, tabelas 1612 (lavouras temporárias) e
             1613 (lavouras permanentes) do PAM (Produção Agrícola Municipal)
Frequência : Anual
Destino    : BigQuery raw.raw_ibge_pam
Execução   : python -m src.ingest_sidra

Consulta a API SIDRA por UF (n3), todas as variáveis de área e produção,
todos os períodos disponíveis, e carrega o JSON normalizado na tabela raw.
"""

import pandas as pd

from config.settings import PROJECT_ID
from src.utils import get_logger, http_get_json, upload_to_bigquery

logger = get_logger(__name__)

SIDRA_BASE_URL = "https://apisidra.ibge.gov.br/values"

_TABLES = {
    # Variáveis 1612: 109=área plantada, 216=área colhida, 214=qtd. produzida, 112=rendimento médio
    "1612": {
        "variaveis": "109,216,214,112",
        "classificacao": "c81",
        "descricao": "lavouras temporárias",
    },
    # Variáveis 1613: 2313=área destinada à colheita (equiv. a 109 nas temporárias),
    # 216=área colhida, 214=qtd. produzida, 112=rendimento médio
    "1613": {
        "variaveis": "2313,216,214,112",
        "classificacao": "c82",
        "descricao": "lavouras permanentes",
    },
}

TABLE_ID = f"{PROJECT_ID}.raw.raw_ibge_pam"

_MISSING_VALUES = {"-", "...", "X", ""}


def fetch_sidra_table(tabela: str) -> list[dict]:
    """Consulta a API SIDRA e retorna os registros brutos para uma tabela PAM.

    Busca dados por UF (nível 3), todas as variáveis de produção agrícola,
    todos os períodos e todos os produtos da classificação correspondente.

    Args:
        tabela: Número da tabela SIDRA ('1612' ou '1613').

    Returns:
        Lista de dicts com os registros JSON retornados pela API SIDRA.
        O primeiro elemento é sempre o cabeçalho da API.

    Raises:
        KeyError: Se a tabela não estiver mapeada em _TABLES.
        requests.HTTPError: Se a resposta HTTP não for 2xx.
    """
    cfg = _TABLES[tabela]
    url = (
        f"{SIDRA_BASE_URL}/t/{tabela}/n3/all"
        f"/v/{cfg['variaveis']}/p/all/{cfg['classificacao']}/0"
    )
    logger.info("Consultando SIDRA tabela %s (%s)...", tabela, cfg["descricao"])
    records = http_get_json(url)
    logger.info("Tabela %s: %d registros brutos recebidos.", tabela, len(records) - 1)
    return records


def normalize_sidra_response(records: list[dict], tabela: str) -> pd.DataFrame:
    """Normaliza os registros brutos da API SIDRA em um DataFrame flat.

    Descarta o primeiro registro (cabeçalho da API) e converte os campos
    de dimensão e valor para colunas nomeadas em snake_case.

    Args:
        records: Lista de dicts retornada pelo endpoint SIDRA.
        tabela: Número da tabela de origem ('1612' ou '1613').

    Returns:
        DataFrame com colunas: tabela, uf_cod, uf_nome, variavel_cod,
        variavel_nome, ano, produto_cod, produto_nome, valor, unidade.
        Retorna DataFrame vazio se records for vazio.
    """
    if not records:
        return pd.DataFrame()

    rows = []
    for rec in records[1:]:  # primeiro elemento é o header da API
        valor_raw = str(rec.get("V", "")).strip()
        if valor_raw in _MISSING_VALUES:
            valor = None
        else:
            try:
                valor = float(valor_raw.replace(",", "."))
            except ValueError:
                valor = None

        rows.append(
            {
                "tabela": tabela,
                "uf_cod": rec.get("D1C"),
                "uf_nome": rec.get("D1N"),
                "variavel_cod": rec.get("D2C"),
                "variavel_nome": rec.get("D2N"),
                "ano": rec.get("D3C"),
                "produto_cod": rec.get("D4C"),
                "produto_nome": rec.get("D4N"),
                "valor": valor,
                "unidade": rec.get("MN"),
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    """Executa a ingestão completa do PAM (tabelas 1612 e 1613) para o BigQuery."""
    all_dfs = []
    for tabela in _TABLES:
        records = fetch_sidra_table(tabela)
        df = normalize_sidra_response(records, tabela)
        logger.info("Tabela %s: %d linhas normalizadas.", tabela, len(df))
        all_dfs.append(df)

    df_combined = pd.concat(all_dfs, ignore_index=True)
    logger.info("Total de linhas a carregar: %d", len(df_combined))
    upload_to_bigquery(df_combined, TABLE_ID)
    logger.info("Carga concluída em %s.", TABLE_ID)


if __name__ == "__main__":
    main()
