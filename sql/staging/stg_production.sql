-- =============================================================
-- stg_production.sql
-- Camada: staging
-- Origem: raw.raw_crop_harvest
-- Destino: staging.stg_production
--
-- Transformações aplicadas:
--   - Extrai o ano final da safra (ex: "2023/24" → 2024)
--   - Padroniza sigla de UF (UPPER + TRIM)
--   - Agrupa "Milho 1ª Safra" e "Milho 2ª Safra" em "milho"
--   - Converte mil hectares → hectares e mil toneladas → toneladas
--   - Filtra registros com produção nula ou zero
-- =============================================================

CREATE OR REPLACE TABLE `safra-brasil-analytics.staging.stg_production` AS

WITH
source AS (
    SELECT
        produto,
        safra,
        UPPER(TRIM(uf)) AS uf_norm,
        area_plantada_mil_ha,
        producao_mil_ton
    FROM `safra-brasil-analytics.raw.raw_crop_harvest`
    WHERE
        producao_mil_ton IS NOT NULL
        AND producao_mil_ton > 0
),

staged AS (
    SELECT
    -- Extrai o ano final da safra: "2023/24" → 2024
    -- Compara o sufixo com os últimos 2 dígitos do ano inicial para detectar
    -- cruzamento de século (ex: "1999/00" → 2000, não 1900)
        CASE
            WHEN CAST(SUBSTR(safra, 6, 2) AS INT64) < CAST(SUBSTR(safra, 3, 2) AS INT64)
                THEN
                    DIV(CAST(LEFT(safra, 4) AS INT64), 100) * 100 + 100
                    + CAST(SUBSTR(safra, 6, 2) AS INT64)
            ELSE
                DIV(CAST(LEFT(safra, 4) AS INT64), 100) * 100
                + CAST(SUBSTR(safra, 6, 2) AS INT64)
        END AS year,

        -- Normaliza o nome da cultura agrupando variantes do mesmo produto.
        -- "Milho 1ª Safra" e "Milho 2ª Safra" viram "milho" para permitir
        -- agregação correta da produção total anual por estado.
        CASE
            WHEN LOWER(produto) LIKE 'milho%' THEN 'milho'
            WHEN LOWER(produto) LIKE 'soja%' THEN 'soja'
            WHEN LOWER(produto) LIKE 'trigo%' THEN 'trigo'
            WHEN LOWER(produto) LIKE 'algodão%' THEN 'algodão'
            WHEN LOWER(produto) LIKE 'arroz%' THEN 'arroz'
            WHEN LOWER(produto) LIKE 'feijão%' THEN 'feijão'
            WHEN LOWER(produto) LIKE 'sorgo%' THEN 'sorgo'
            WHEN LOWER(produto) LIKE 'cana%' THEN 'cana-de-açúcar'
            ELSE LOWER(REGEXP_REPLACE(produto, r'\s*\(.*?\)', ''))
        END AS crop_name,

        uf_norm AS state_code,

        area_plantada_mil_ha * 1000 AS harvested_area_ha, -- mil ha → ha
        producao_mil_ton * 1000 AS production_ton     -- mil t  → t

    FROM source
)

SELECT
    year,
    crop_name,
    state_code,
    -- Soma milho 1ª e 2ª safra (agora com o mesmo crop_name) em um único registro
    SUM(harvested_area_ha) AS harvested_area_ha,
    SUM(production_ton) AS production_ton
FROM staged
GROUP BY year, crop_name, state_code
