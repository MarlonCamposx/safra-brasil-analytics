-- =============================================================
-- dim_crop.sql
-- Camada: gold
-- Origem: staging.stg_production
-- Destino: gold.dim_crop
--
-- Transformações aplicadas:
--   - Mapeamento de cultura para rótulo amigável
-- =============================================================

CREATE OR REPLACE TABLE `safra-brasil-analytics.gold.dim_crop` AS

SELECT
    crop_name,
    CASE crop_name
        WHEN 'milho' THEN 'Milho'
        WHEN 'soja' THEN 'Soja'
        WHEN 'trigo' THEN 'Trigo'
        WHEN 'algodão' THEN 'Algodão'
        WHEN 'arroz' THEN 'Arroz'
        WHEN 'feijão' THEN 'Feijão'
        WHEN 'sorgo' THEN 'Sorgo'
        WHEN 'cana-de-açúcar' THEN 'Cana-de-Açúcar'
        ELSE 'Desconhecido'
    END AS crop_label

FROM (
    SELECT DISTINCT crop_name
    FROM `safra-brasil-analytics.staging.stg_production`
)

ORDER BY crop_name;
