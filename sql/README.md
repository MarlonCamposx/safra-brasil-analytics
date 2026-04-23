# SQL Execution Guide

## Layer Overview

| Layer   | Dataset   | Description                              |
|---------|-----------|------------------------------------------|
| staging | `staging` | Raw data landed from source systems      |
| gold    | `gold`    | Business-ready, aggregated tables        |

## Execution Order

Scripts must be executed in the following order to respect dependencies:

### 1. Staging

> Run first. These scripts create the base tables from raw sources.

```
sql/staging/
```

### 2. Gold

> Run after staging is complete. These scripts read from staging tables.

```
sql/gold/
```

## Dependencies

- Gold tables depend on staging tables being populated.
- All queries target the BigQuery project defined in `config/settings.py` (`PROJECT_ID`).
