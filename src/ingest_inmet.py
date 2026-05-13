"""
ingest_inmet.py
---------------
Fonte      : INMET — API pública de estações meteorológicas
             (https://apitempo.inmet.gov.br)
Frequência : Sob demanda (catálogo de estações atualizado periodicamente)
Destino    : BigQuery raw.raw_weather_station, raw.raw_weather_measurement
Execução   : python -m src.ingest_inmet

Baixa o catálogo completo de estações meteorológicas (automáticas e
convencionais), normaliza os campos e carrega na tabela raw do BigQuery.
Também ingere medições históricas de precipitação por estação.
"""

import pandas as pd

from config.settings import PROJECT_ID
from src.utils import get_logger, http_get_json, upload_to_bigquery

logger = get_logger(__name__)

INMET_BASE_URL = "https://apitempo.inmet.gov.br"

# T = automáticas, M = convencionais (manuais)
_STATION_TYPES = ["T", "M"]

TABLE_ID = f"{PROJECT_ID}.raw.raw_weather_station"
TABLE_ID_MEASUREMENTS = f"{PROJECT_ID}.raw.raw_weather_measurement"

MEASUREMENTS_START_DATE = "2020-01-01"
MEASUREMENTS_END_DATE = "2024-12-31"


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


def fetch_measurements(cd_estacao: str, data_inicio: str, data_fim: str) -> list[dict]:
    """Consulta a API INMET e retorna as medições históricas de uma estação.

    Args:
        cd_estacao: Código da estação meteorológica (ex: 'A001').
        data_inicio: Data de início no formato 'YYYY-MM-DD'.
        data_fim: Data de fim no formato 'YYYY-MM-DD'.

    Returns:
        Lista de dicts com os registros de medição da estação.

    Raises:
        requests.HTTPError: Se a resposta HTTP não for 2xx.
    """
    url = f"{INMET_BASE_URL}/estacao/historico/{data_inicio}/{data_fim}/{cd_estacao}"
    logger.info("Buscando medições da estação %s (%s a %s)...", cd_estacao, data_inicio, data_fim)
    records = http_get_json(url)
    logger.info("%d medições recebidas para estação %s.", len(records), cd_estacao)
    return records


def normalize_measurements(records: list[dict]) -> pd.DataFrame:
    """Normaliza os registros brutos de medição da API INMET em um DataFrame flat.

    Args:
        records: Lista de dicts retornada pelo endpoint /estacao/historico/.

    Returns:
        DataFrame com colunas: cd_estacao, measurement_date, precip_mm.
        Retorna DataFrame vazio se records for vazio.
    """
    if not records:
        return pd.DataFrame()

    rows = []
    for rec in records:
        rows.append(
            {
                "cd_estacao": rec.get("CD_ESTACAO"),
                "measurement_date": rec.get("DT_MEDICAO"),
                "precip_mm": _to_float(rec.get("CHUVA")),
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
    """Executa a ingestão do catálogo de estações e medições históricas do INMET."""
    all_dfs = []
    for station_type in _STATION_TYPES:
        records = fetch_stations(station_type)
        df = normalize_stations(records)
        logger.info("Tipo %s: %d estações normalizadas.", station_type, len(df))
        all_dfs.append(df)

    df_stations = pd.concat(all_dfs, ignore_index=True)
    logger.info("Total de estações a carregar: %d", len(df_stations))
    upload_to_bigquery(df_stations, TABLE_ID)
    logger.info("Carga concluída em %s.", TABLE_ID)

    all_measurements = []
    for cd_estacao in df_stations["cd_estacao"].dropna().unique():
        records = fetch_measurements(cd_estacao, MEASUREMENTS_START_DATE, MEASUREMENTS_END_DATE)
        df = normalize_measurements(records)
        if not df.empty:
            all_measurements.append(df)

    if all_measurements:
        df_measurements = pd.concat(all_measurements, ignore_index=True)
        logger.info("Total de medições a carregar: %d", len(df_measurements))
        upload_to_bigquery(df_measurements, TABLE_ID_MEASUREMENTS)
        logger.info("Carga concluída em %s.", TABLE_ID_MEASUREMENTS)
    else:
        logger.warning("Nenhuma medição retornada pela API.")


if __name__ == "__main__":
    main()
