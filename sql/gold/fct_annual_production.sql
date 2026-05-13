-- =============================================================
-- fct_annual_production.sql
-- Camada: gold
-- Origem: staging.stg_production, gold.dim_state, gold.dim_crop, gold.dim_time
-- Destino: gold.fct_annual_production
--
-- Transformações aplicadas:
--   - Tabela fato de produção anual, combinando dados de produção
--     com dimensões de estado, cultura e tempo
-- =============================================================

CREATE OR REPLACE TABLE `safra-brasil-analytics.gold.fct_annual_production` AS

WITH
stg_production AS (
    SELECT
        year,
        state_code,
        crop_name,
        harvested_area_ha,
        production_ton,
        -- Cálculo de produtividade ao ano (toneladas por hectare)
        CASE
            WHEN harvested_area_ha > 0 THEN ROUND(production_ton / harvested_area_ha, 2)
        END AS yield_ton_per_ha,
        -- Produção em toneladas comparando com o ano anterior (Função LAG)
        production_ton
        - LAG(production_ton)
            OVER (PARTITION BY state_code, crop_name ORDER BY year)
            AS yoy_change
    FROM `safra-brasil-analytics.staging.stg_production`
),

dim_state AS (
    SELECT
        state_code,
        state_name,
        region
    FROM `safra-brasil-analytics.gold.dim_state`
),

dim_crop AS (
    SELECT
        crop_name,
        crop_label
    FROM `safra-brasil-analytics.gold.dim_crop`
),

dim_time AS (
    SELECT
        year,
        decade
    FROM `safra-brasil-analytics.gold.dim_time`
)

SELECT
    sp.year,
    dt.decade,
    sp.state_code,
    ds.state_name,
    ds.region,
    sp.crop_name,
    dc.crop_label,
    sp.harvested_area_ha,
    sp.production_ton,
    sp.yield_ton_per_ha,
    sp.yoy_change
FROM stg_production AS sp
INNER JOIN dim_state AS ds ON sp.state_code = ds.state_code
INNER JOIN dim_crop AS dc ON sp.crop_name = dc.crop_name
INNER JOIN dim_time AS dt ON sp.year = dt.year

ORDER BY sp.year, sp.state_code, sp.crop_name;
