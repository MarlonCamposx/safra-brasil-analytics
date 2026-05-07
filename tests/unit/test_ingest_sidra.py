import pandas as pd
import pytest

from src.ingest_sidra import fetch_sidra_table, normalize_sidra_response


class TestNormalizeSidraResponse:
    """Testa a normalização dos registros brutos da API SIDRA."""

    def _api_records(self):
        return [
            # primeiro registro é o header da API — deve ser descartado
            {
                "NC": "Nível Geográfico",
                "NN": "Nome Geográfico",
                "MC": "Unidade de Medida",
                "MN": "Nome da Unidade de Medida",
                "V": "Valor",
                "D1C": "Cód. UF",
                "D1N": "UF",
                "D2C": "Cód. Variável",
                "D2N": "Variável",
                "D3C": "Cód. Ano",
                "D3N": "Ano",
                "D4C": "Cód. Produto",
                "D4N": "Produto",
            },
            {
                "NC": "3",
                "NN": "Unidade da Federação",
                "MC": "1006",
                "MN": "Hectares",
                "V": "523000",
                "D1C": "35",
                "D1N": "São Paulo",
                "D2C": "109",
                "D2N": "Área plantada",
                "D3C": "2023",
                "D3N": "2023",
                "D4C": "2736",
                "D4N": "Soja (em grão)",
            },
            {
                "NC": "3",
                "NN": "Unidade da Federação",
                "MC": "1006",
                "MN": "Toneladas",
                "V": "-",
                "D1C": "51",
                "D1N": "Mato Grosso",
                "D2C": "214",
                "D2N": "Quantidade produzida",
                "D3C": "2023",
                "D3N": "2023",
                "D4C": "2736",
                "D4N": "Soja (em grão)",
            },
        ]

    def test_skips_api_header_row(self):
        df = normalize_sidra_response(self._api_records(), "1612")
        assert len(df) == 2

    def test_empty_records_returns_empty_dataframe(self):
        df = normalize_sidra_response([], "1612")
        assert df.empty

    def test_adds_tabela_column(self):
        df = normalize_sidra_response(self._api_records(), "1612")
        assert (df["tabela"] == "1612").all()

    def test_maps_uf_fields(self):
        df = normalize_sidra_response(self._api_records(), "1612")
        assert "35" in df["uf_cod"].values
        assert "São Paulo" in df["uf_nome"].values

    def test_converts_numeric_value(self):
        df = normalize_sidra_response(self._api_records(), "1612")
        row = df[df["uf_cod"] == "35"].iloc[0]
        assert row["valor"] == 523000.0

    def test_missing_value_dash_becomes_none(self):
        df = normalize_sidra_response(self._api_records(), "1612")
        row = df[df["uf_cod"] == "51"].iloc[0]
        assert row["valor"] is None or pd.isna(row["valor"])

    def test_output_has_expected_columns(self):
        df = normalize_sidra_response(self._api_records(), "1612")
        expected = {
            "tabela",
            "uf_cod",
            "uf_nome",
            "variavel_cod",
            "variavel_nome",
            "ano",
            "produto_cod",
            "produto_nome",
            "valor",
            "unidade",
        }
        assert expected.issubset(set(df.columns))


class TestFetchSidraTable:
    """Testa a chamada à API SIDRA — sem chamadas reais de rede."""

    def test_calls_correct_url_for_1612(self, mocker):
        mock_http = mocker.patch(
            "src.ingest_sidra.http_get_json",
            return_value=[{"header": True}, {"D1C": "11"}],
        )
        fetch_sidra_table("1612")
        url_called = mock_http.call_args[0][0]
        assert "t/1612" in url_called
        assert "c81" in url_called

    def test_calls_correct_url_for_1613(self, mocker):
        mock_http = mocker.patch(
            "src.ingest_sidra.http_get_json",
            return_value=[{"header": True}],
        )
        fetch_sidra_table("1613")
        url_called = mock_http.call_args[0][0]
        assert "t/1613" in url_called
        assert "c82" in url_called

    def test_raises_for_unknown_table(self):
        with pytest.raises(KeyError):
            fetch_sidra_table("9999")

    def test_returns_raw_records(self, mocker):
        fake_records = [{"header": True}, {"D1C": "35", "V": "100"}]
        mocker.patch("src.ingest_sidra.http_get_json", return_value=fake_records)
        result = fetch_sidra_table("1612")
        assert result == fake_records
