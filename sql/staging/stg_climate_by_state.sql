-- =============================================================
-- stg_climate_by_state.sql
-- Camada: staging
-- Origem: raw.raw_weather_station
-- Destino: staging.stg_climate_by_state
--
-- Transformações aplicadas:
--   - Padroniza sigla de UF (UPPER + TRIM de sg_estado)
--   - Extrai o ano de início de operação de cada estação
--   - Agrega número de estações meteorológicas ativas por estado e ano
--
-- NOTA: Agregação de precipitação pluviométrica por estado/ano requer
--   dados de medição histórica da API INMET (endpoint /historico).
--   O ingest_inmet.py atual carrega apenas o catálogo de estações.
--   Quando o script for expandido para ingerir medições, substituir
--   a agregação de contagem pela média de precipitação anual.
-- =============================================================

CREATE OR REPLACE TABLE `safra-brasil-analytics.staging.stg_climate_by_state` AS

WITH
source AS (
    SELECT
        cd_estacao,
        UPPER(TRIM(sg_estado)) AS state_code, -- normaliza para sigla de 2 letras maiúsculas
        tp_estacao,
        dt_inicio_operacao,
        dt_fim_operacao,
        cd_situacao
    FROM `safra-brasil-analytics.raw.raw_weather_station`
    WHERE
        sg_estado IS NOT NULL
        AND sg_estado != ''
),

with_year AS (
    SELECT
        cd_estacao,
        state_code,
        tp_estacao,
        cd_situacao,
        -- Extrai o ano de início a partir da string "YYYY-MM-DD".
        -- Estações sem data de início são descartadas (não têm vínculo temporal).
        SAFE_CAST(LEFT(dt_inicio_operacao, 4) AS INT64) AS operation_start_year,
        SAFE_CAST(LEFT(dt_fim_operacao, 4) AS INT64) AS operation_end_year
    FROM source
    WHERE
        dt_inicio_operacao IS NOT NULL
        AND LENGTH(dt_inicio_operacao) >= 4
)

SELECT
    state_code,
    operation_start_year AS year,
    COUNT(cd_estacao) AS station_count,
    -- Filtra por tipo para distinguir cobertura automática da convencional
    COUNTIF(tp_estacao = 'T') AS automatic_stations,
    COUNTIF(tp_estacao = 'M') AS manual_stations,
    COUNTIF(cd_situacao = 'Ativa') AS active_stations
FROM with_year
WHERE operation_start_year IS NOT NULL
GROUP BY state_code, year
