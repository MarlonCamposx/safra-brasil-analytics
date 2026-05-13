-- =============================================================
-- dim_state.sql
-- Camada: gold
-- Origem: staging.stg_production
-- Destino: gold.dim_state
--
-- Transformações aplicadas:
--   - Mapeamento de códigos de estado para nomes completos
--   - Classificação dos estados em regiões geográficas
-- =============================================================

CREATE OR REPLACE TABLE `safra-brasil-analytics.gold.dim_state` AS

SELECT
    state_code,
    CASE state_code
        WHEN 'AC' THEN 'Acre'
        WHEN 'AL' THEN 'Alagoas'
        WHEN 'AM' THEN 'Amazonas'
        WHEN 'AP' THEN 'Amapá'
        WHEN 'BA' THEN 'Bahia'
        WHEN 'CE' THEN 'Ceará'
        WHEN 'DF' THEN 'Distrito Federal'
        WHEN 'ES' THEN 'Espírito Santo'
        WHEN 'GO' THEN 'Goiás'
        WHEN 'MA' THEN 'Maranhão'
        WHEN 'MG' THEN 'Minas Gerais'
        WHEN 'MS' THEN 'Mato Grosso do Sul'
        WHEN 'MT' THEN 'Mato Grosso'
        WHEN 'PA' THEN 'Pará'
        WHEN 'PB' THEN 'Paraíba'
        WHEN 'PE' THEN 'Pernambuco'
        WHEN 'PI' THEN 'Piauí'
        WHEN 'PR' THEN 'Paraná'
        WHEN 'RJ' THEN 'Rio de Janeiro'
        WHEN 'RN' THEN 'Rio Grande do Norte'
        WHEN 'RO' THEN 'Rondônia'
        WHEN 'RR' THEN 'Roraima'
        WHEN 'RS' THEN 'Rio Grande do Sul'
        WHEN 'SC' THEN 'Santa Catarina'
        WHEN 'SE' THEN 'Sergipe'
        WHEN 'SP' THEN 'São Paulo'
        WHEN 'TO' THEN 'Tocantins'
        ELSE 'Desconhecido'
    END AS state_name,

    CASE
        WHEN state_code IN ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
        WHEN state_code IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
        WHEN state_code IN ('DF', 'GO', 'MS', 'MT') THEN 'Centro-Oeste'
        WHEN state_code IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
        WHEN state_code IN ('PR', 'RS', 'SC') THEN 'Sul'
        ELSE 'Desconecido'
    END AS region

FROM (
    SELECT DISTINCT state_code
    FROM `safra-brasil-analytics.staging.stg_production`
)

ORDER BY state_code;
