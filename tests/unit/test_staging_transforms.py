import pytest

from src.staging_transforms import calculate_yield, extract_harvest_year, normalize_crop_name


class TestExtractHarvestYear:
    """Testa o parsing do ano final da safra a partir do formato 'AAAA/AA'."""

    def test_standard_format(self):
        assert extract_harvest_year("2023/24") == 2024

    def test_century_boundary(self):
        """'1999/00' deve retornar 2000, não 1900."""
        assert extract_harvest_year("1999/00") == 2000

    def test_single_year_format(self):
        """Algumas safras podem vir como ano único — deve retornar o próprio ano."""
        assert extract_harvest_year("2023") == 2023

    def test_invalid_format_raises_value_error(self):
        with pytest.raises(ValueError, match="formato inválido"):
            extract_harvest_year("safra-invalida")

    def test_none_raises_value_error(self):
        with pytest.raises(ValueError):
            extract_harvest_year(None)


class TestNormalizeCropName:
    """Testa a normalização e agrupamento de nomes de culturas."""

    def test_milho_first_harvest(self):
        assert normalize_crop_name("Milho 1ª Safra") == "milho"

    def test_milho_second_harvest(self):
        """Milho 2ª safra (safrinha) deve ser agrupado com milho."""
        assert normalize_crop_name("Milho 2ª Safra") == "milho"

    def test_soja(self):
        assert normalize_crop_name("Soja (em grão)") == "soja"

    def test_unknown_crop_returns_lowercase(self):
        """Culturas desconhecidas devem ser retornadas em lowercase sem modificação."""
        assert normalize_crop_name("Feijão") == "feijão"


class TestCalculateYield:
    """Testa o cálculo de produtividade (ton/ha)."""

    def test_valid_calculation(self):
        assert calculate_yield(production_ton=3000.0, harvested_area_ha=1000.0) == 3.0

    def test_zero_area_returns_none(self):
        """Divisão por zero deve retornar None, não levantar exceção."""
        assert calculate_yield(production_ton=1000.0, harvested_area_ha=0.0) is None

    def test_none_production_returns_none(self):
        assert calculate_yield(production_ton=None, harvested_area_ha=1000.0) is None

    def test_none_area_returns_none(self):
        assert calculate_yield(production_ton=1000.0, harvested_area_ha=None) is None

    def test_result_precision(self):
        """Verifica precisão até 4 casas decimais."""
        result = calculate_yield(production_ton=1000.0, harvested_area_ha=3000.0)
        assert round(result, 4) == 0.3333
