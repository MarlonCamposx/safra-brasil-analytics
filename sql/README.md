# SQL Execution Guide

## Layer Overview

| Layer   | Dataset   | Description                              |
|---------|-----------|------------------------------------------|
| staging | `staging` | Raw data landed from source systems      |
| gold    | `gold`    | Business-ready, aggregated tables        |

## Execution Order

Scripts must be executed in the following order to respect dependencies (staging first, then dimensions, then facts):

### 1. Staging

> Run first. These scripts create the base tables from raw sources.

| # | File | Destination |
|---|------|-------------|
| 1 | `sql/staging/stg_production.sql` | `staging.stg_production` |
| 2 | `sql/staging/stg_climate_by_state.sql` | `staging.stg_climate_by_state` |

### 2. Gold — Dimensions

> Run after staging. Dimensions must exist before facts.

| # | File | Destination |
|---|------|-------------|
| 3 | `sql/gold/dim_state.sql` | `gold.dim_state` |
| 4 | `sql/gold/dim_crop.sql` | `gold.dim_crop` |
| 5 | `sql/gold/dim_time.sql` | `gold.dim_time` |

### 3. Gold — Facts

> Run last. Facts depend on both staging and dimension tables.

| # | File | Destination |
|---|------|-------------|
| 6 | `sql/gold/fct_annual_production.sql` | `gold.fct_annual_production` |
| 7 | `sql/gold/fct_annual_climate.sql` | `gold.fct_annual_climate` |

## Dependencies

- Staging tables depend on raw tables being populated (`raw.raw_production`, `raw.raw_weather_station`).
- Dimension tables depend on staging tables being populated.
- Fact tables depend on both staging and dimension tables.
- All queries target the BigQuery project defined in `config/settings.py` (`PROJECT_ID`).
