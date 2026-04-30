"""
conftest.py
-----------
Fixtures compartilhadas para a suíte de testes do projeto
Safra Agrícola Brasil.
"""

import pandas as pd
import pytest


@pytest.fixture
def sample_raw_harvest_df():
    """DataFrame simulando raw_crop_harvest após ingestão CONAB.

    Contém intencionalmente valores sujos para testar a limpeza:
    - UFs em minúsculas e com espaços
    - Linha com producao=None
    - Formato de safra "AAAA/AA"
    """
    return pd.DataFrame(
        {
            "safra": ["2022/23", "2023/24", "2023/24"],
            "produto": ["Soja (em grão)", "Milho 1ª Safra", "Milho 2ª Safra"],
            "uf": ["mt", "sp", "  GO "],
            "area_plantada_mil_ha": [10000.0, 5000.0, 3000.0],
            "producao_mil_ton": [30000.0, 15000.0, None],
        }
    )


@pytest.fixture
def sample_stg_production_df():
    """DataFrame simulando stg_production após transformações de staging.

    Representa o estado limpo e padronizado esperado após processar
    o sample_raw_harvest_df acima.
    """
    return pd.DataFrame(
        {
            "year": [2023, 2024, 2024],
            "crop_name": ["soja", "milho", "milho"],
            "state_code": ["MT", "SP", "GO"],
            "harvested_area_ha": [10_000_000.0, 5_000_000.0, 3_000_000.0],
            "production_ton": [30_000_000.0, 15_000_000.0, 9_000_000.0],
        }
    )
