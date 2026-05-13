-- =============================================================
-- dim_time.sql
-- Camada: gold
-- Origem: staging.stg_production
-- Destino: gold.dim_time
--
-- Transformações aplicadas:
--   - Criação de décadas com base no ano de produção
-- =============================================================

CREATE OR REPLACE TABLE `safra-brasil-analytics.gold.dim_time` AS

SELECT
    year,
    DIV(`year`, 10) * 10 AS decade

FROM (
    SELECT DISTINCT year
    FROM `safra-brasil-analytics.staging.stg_production`
)

ORDER BY year;
