"""
ingest_inmet.py
---------------
Fonte      : INMET — API pública de estações meteorológicas
             (https://apitempo.inmet.gov.br)
Frequência : Sob demanda (catálogo de estações atualizado periodicamente)
Destino    : BigQuery raw.raw_weather_station
Execução   : python -m src.ingest_inmet

Baixa o catálogo completo de estações meteorológicas (automáticas e
convencionais), normaliza os campos e carrega na tabela raw do BigQuery.
"""

import pandas as pd

from config.settings import PROJECT_ID
from src.utils import get_logger, http_get_json, upload_to_bigquery

logger = get_logger(__name__)

INMET_BASE_URL = "https://apitempo.inmet.gov.br"

# T = automáticas, M = convencionais (manuais)
_STATION_TYPES = ["T", "M"]

TABLE_ID = f"{PROJECT_ID}.raw.raw_weather_station"


def fetch_stations(station_type: str) -> list[dict]:
    """Consulta a API INMET e retorna a lista de estações de um tipo.

    Args:
        station_type: Tipo de estação — 'T' para automáticas, 'M' para
            convencionais/manuais.

    Returns:
        Lista de dicts com os atributos de cada estação meteorológica.

    Raises:
        requests.HTTPError: Se a resposta HTTP não for 2xx.
    """
    url = f"{INMET_BASE_URL}/estacoes/{station_type}"
    label = "automáticas" if station_type == "T" else "convencionais"
    logger.info("Buscando estações %s do INMET...", label)
    records = http_get_json(url)
    logger.info("%d estações %s recebidas.", len(records), label)
    return records


def normalize_stations(records: list[dict]) -> pd.DataFrame:
    """Normaliza os registros brutos da API INMET em um DataFrame flat.

    Converte chaves da API (SCREAMING_SNAKE_CASE) para snake_case e
    campos numéricos de latitude, longitude e altitude para float.

    Args:
        records: Lista de dicts retornada pelo endpoint /estacoes/{tipo}.

    Returns:
        DataFrame com colunas: cd_estacao, dc_nome, sg_estado, tp_estacao,
        vl_latitude, vl_longitude, vl_altitude, dt_inicio_operacao,
        dt_fim_operacao, cd_situacao, fl_capital, cd_oscar, cd_distrito,
        sg_entidade, cd_wsi.
        Retorna DataFrame vazio se records for vazio.
    """
    if not records:
        return pd.DataFrame()

    rows = []
    for rec in records:
        rows.append(
            {
                "cd_estacao": rec.get("CD_ESTACAO"),
                "dc_nome": rec.get("DC_NOME"),
                "sg_estado": rec.get("SG_ESTADO"),
                "tp_estacao": rec.get("TP_ESTACAO"),
                "vl_latitude": _to_float(rec.get("VL_LATITUDE")),
                "vl_longitude": _to_float(rec.get("VL_LONGITUDE")),
                "vl_altitude": _to_float(rec.get("VL_ALTITUDE")),
                "dt_inicio_operacao": rec.get("DT_INICIO_OPERACAO"),
                "dt_fim_operacao": rec.get("DT_FIM_OPERACAO"),
                "cd_situacao": rec.get("CD_SITUACAO"),
                "fl_capital": rec.get("FL_CAPITAL"),
                "cd_oscar": rec.get("CD_OSCAR"),
                "cd_distrito": rec.get("CD_DISTRITO"),
                "sg_entidade": rec.get("SG_ENTIDADE"),
                "cd_wsi": rec.get("CD_WSI"),
            }
        )

    return pd.DataFrame(rows)


def _to_float(value) -> float | None:
    """Converte um valor de string para float, retornando None em caso de falha.

    Args:
        value: Valor a converter — pode ser string, int, float ou None.

    Returns:
        Float convertido ou None se a conversão não for possível.
    """
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "."))
    except (ValueError, TypeError):
        return None


def main() -> None:
    """Executa a ingestão do catálogo de estações INMET para o BigQuery."""
    all_dfs = []
    for station_type in _STATION_TYPES:
        records = fetch_stations(station_type)
        df = normalize_stations(records)
        logger.info("Tipo %s: %d estações normalizadas.", station_type, len(df))
        all_dfs.append(df)

    df_combined = pd.concat(all_dfs, ignore_index=True)
    logger.info("Total de estações a carregar: %d", len(df_combined))
    upload_to_bigquery(df_combined, TABLE_ID)
    logger.info("Carga concluída em %s.", TABLE_ID)


if __name__ == "__main__":
    main()
