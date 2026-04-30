import pandas as pd
import pytest

from src.ingest_conab import load_to_bigquery, normalize_state_code


class TestNormalizeStateCode:
    """Testa a normalização de códigos de UF."""

    def test_lowercase_uf(self):
        assert normalize_state_code("sp") == "SP"

    def test_strips_whitespace(self):
        assert normalize_state_code("  MT ") == "MT"

    def test_known_full_name(self):
        """Nome completo deve ser mapeado para sigla."""
        assert normalize_state_code("Mato Grosso") == "MT"

    def test_already_normalized(self):
        assert normalize_state_code("GO") == "GO"

    def test_none_raises_value_error(self):
        with pytest.raises(ValueError, match="state code cannot be None"):
            normalize_state_code(None)

    def test_empty_string_raises_value_error(self):
        with pytest.raises(ValueError):
            normalize_state_code("")


class TestLoadToBigQuery:
    """Testa o upload de DataFrame para BigQuery — sem chamadas reais."""

    def test_calls_load_table_from_dataframe(self, mocker):
        """Verifica que o método correto é chamado no client do BigQuery."""
        mock_client = mocker.patch("src.ingest_conab.bigquery.Client")
        mock_instance = mock_client.return_value

        df = pd.DataFrame({"col": [1, 2, 3]})
        load_to_bigquery(df, "raw.raw_crop_harvest")

        mock_instance.load_table_from_dataframe.assert_called_once()

    def test_passes_correct_table_id(self, mocker):
        """Verifica que o table_id passado é o mesmo recebido pelo client."""
        mock_client = mocker.patch("src.ingest_conab.bigquery.Client")
        mock_instance = mock_client.return_value

        df = pd.DataFrame({"col": [1]})
        load_to_bigquery(df, "raw.raw_crop_harvest")

        call_args = mock_instance.load_table_from_dataframe.call_args
        assert "raw.raw_crop_harvest" in str(call_args)
