-- =============================================================
-- fct_annual_climate.sql
-- Camada: gold
-- Origem: staging.stg_climate_by_state, gold.dim_state, gold.dim_time
-- Destino: gold.fct_annual_climate
--
-- Transformações aplicadas:
--   - Tabela fato de clima anual, combinando cobertura de estações meteorológicas
--     com dimensões de estado e tempo
--
-- LIMITAÇÃO: avg_precip_mm (média de precipitação anual por estado) não está disponível
--   pois ingest_inmet.py carrega apenas o catálogo de estações, não medições históricas.
--   Quando o script for expandido para ingerir medições, adicionar avg_precip_mm aqui.
-- =============================================================

CREATE OR REPLACE TABLE `safra-brasil-analytics.gold.fct_annual_climate` AS

WITH
stg_climate AS (
    SELECT
        year,
        state_code,
        station_count,
        automatic_stations,
        manual_stations,
        active_stations
    FROM `safra-brasil-analytics.staging.stg_climate_by_state`
),

dim_time AS (
    SELECT
        year,
        decade
    FROM `safra-brasil-analytics.gold.dim_time`
),

dim_state AS (
    SELECT
        state_code,
        state_name,
        region
    FROM `safra-brasil-analytics.gold.dim_state`
)

SELECT
    sc.year,
    dt.decade,
    sc.state_code,
    ds.state_name,
    ds.region,
    sc.station_count,
    sc.automatic_stations,
    sc.manual_stations,
    sc.active_stations
FROM stg_climate AS sc
INNER JOIN dim_time AS dt ON sc.year = dt.year
INNER JOIN dim_state AS ds ON sc.state_code = ds.state_code

ORDER BY sc.year, sc.state_code;
