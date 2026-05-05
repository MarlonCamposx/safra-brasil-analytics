import pandas as pd
import pytest

from src.ingest_conab import (
    _transform_sheet,
    download_conab_xls,
    load_to_bigquery,
    normalize_state_code,
    parse_crop_content,
)


class TestNormalizeStateCode:
    """Testa a normalização de códigos de UF."""

    def test_lowercase_uf(self):
        assert normalize_state_code("sp") == "SP"

    def test_strips_whitespace(self):
        assert normalize_state_code("  MT ") == "MT"

    def test_known_full_name(self):
        assert normalize_state_code("Mato Grosso") == "MT"

    def test_already_normalized(self):
        assert normalize_state_code("GO") == "GO"

    def test_none_raises_value_error(self):
        with pytest.raises(ValueError, match="state code cannot be None"):
            normalize_state_code(None)

    def test_empty_string_raises_value_error(self):
        with pytest.raises(ValueError):
            normalize_state_code("")

    def test_unknown_full_name_returns_uppercase(self):
        """Nome desconhecido deve retornar em maiúsculas sem mapeamento."""
        assert normalize_state_code("Unknownland") == "UNKNOWNLAND"


class TestLoadToBigQuery:
    """Testa o upload de DataFrame para BigQuery — sem chamadas reais."""

    def test_calls_load_table_from_dataframe(self, mocker):
        mock_client = mocker.patch("src.ingest_conab.bigquery.Client")
        mock_instance = mock_client.return_value

        df = pd.DataFrame({"col": [1, 2, 3]})
        load_to_bigquery(df, "raw.raw_crop_harvest")

        mock_instance.load_table_from_dataframe.assert_called_once()

    def test_passes_correct_table_id(self, mocker):
        mock_client = mocker.patch("src.ingest_conab.bigquery.Client")
        mock_instance = mock_client.return_value

        df = pd.DataFrame({"col": [1]})
        load_to_bigquery(df, "raw.raw_crop_harvest")

        call_args = mock_instance.load_table_from_dataframe.call_args
        assert "raw.raw_crop_harvest" in str(call_args)


class TestDownloadConabXls:
    """Testa o download do XLS da CONAB — sem chamadas reais de rede."""

    def test_skips_download_if_file_exists(self, mocker, tmp_path):
        existing = tmp_path / "conab_soja_serie_historica.xls"
        existing.write_bytes(b"fake")
        mock_get = mocker.patch("src.ingest_conab.requests.get")

        result = download_conab_xls("Soja", "http://fake-url", dest_dir=str(tmp_path))

        mock_get.assert_not_called()
        assert result == existing

    def test_downloads_and_saves_file(self, mocker, tmp_path):
        mock_response = mocker.MagicMock()
        mock_response.content = b"xls-content"
        mock_response.headers.get.return_value = "application/vnd.ms-excel"
        mocker.patch("src.ingest_conab.requests.get", return_value=mock_response)

        result = download_conab_xls("Soja", "http://fake-url", dest_dir=str(tmp_path))

        assert result.exists()
        assert result.read_bytes() == b"xls-content"

    def test_raises_on_http_error(self, mocker, tmp_path):
        import requests as req

        mock_response = mocker.MagicMock()
        mock_response.raise_for_status.side_effect = req.HTTPError("404")
        mocker.patch("src.ingest_conab.requests.get", return_value=mock_response)

        with pytest.raises(req.HTTPError):
            download_conab_xls("Soja", "http://bad-url", dest_dir=str(tmp_path))

    def test_raises_on_html_response(self, mocker, tmp_path):
        mock_response = mocker.MagicMock()
        mock_response.content = b"<!DOCTYPE html><html>page</html>"
        mock_response.headers.get.return_value = "text/html;charset=utf-8"
        mocker.patch("src.ingest_conab.requests.get", return_value=mock_response)

        with pytest.raises(RuntimeError, match="HTML"):
            download_conab_xls("Soja", "http://bad-url", dest_dir=str(tmp_path))


class TestParseCropContent:
    """Testa a orquestração de leitura e combinação das 3 abas do XLS."""

    def _wide_df(self):
        return pd.DataFrame(
            {
                "REGIAO/UF": ["NORTE", "SP", "MG"],
                "2022/23": ["100", "50", "30"],
            }
        )

    def test_adds_produto_column(self, mocker):
        mocker.patch("src.ingest_conab.pd.read_excel", return_value=self._wide_df())
        df = parse_crop_content(b"fake", "Soja")
        assert "produto" in df.columns
        assert (df["produto"] == "Soja").all()

    def test_output_has_all_metric_columns(self, mocker):
        mocker.patch("src.ingest_conab.pd.read_excel", return_value=self._wide_df())
        df = parse_crop_content(b"fake", "Milho")
        for col in ["area_plantada_mil_ha", "produtividade_kg_ha", "producao_mil_ton"]:
            assert col in df.columns

    def test_filters_regional_rows(self, mocker):
        mocker.patch("src.ingest_conab.pd.read_excel", return_value=self._wide_df())
        df = parse_crop_content(b"fake", "Soja")
        assert "NORTE" not in df["uf"].values


class TestTransformSheet:
    """Testa a conversão de wide para long format."""

    def _wide_df(self):
        return pd.DataFrame(
            {
                "REGIAO/UF": ["NORTE", "SP", "MG", "RO", None],
                "2022/23": ["100", "50", "30", "10", None],
                "2023/24 Previsão (¹)": ["120", "60", "35", "12", None],
            }
        )

    def test_filters_out_regional_rows(self):
        df = _transform_sheet(self._wide_df(), "area_plantada_mil_ha")
        assert "NORTE" not in df["uf"].values

    def test_keeps_valid_uf_rows(self):
        df = _transform_sheet(self._wide_df(), "area_plantada_mil_ha")
        assert set(df["uf"].unique()) == {"SP", "MG", "RO"}

    def test_drops_nan_uf_rows(self):
        df = _transform_sheet(self._wide_df(), "area_plantada_mil_ha")
        assert df["uf"].isna().sum() == 0

    def test_extracts_safra_year_only(self):
        df = _transform_sheet(self._wide_df(), "area_plantada_mil_ha")
        assert "2023/24 Previsão (¹)" not in df["safra"].values
        assert "2023/24" in df["safra"].values

    def test_metric_column_is_numeric(self):
        df = _transform_sheet(self._wide_df(), "area_plantada_mil_ha")
        assert pd.api.types.is_numeric_dtype(df["area_plantada_mil_ha"])

    def test_output_has_expected_columns(self):
        df = _transform_sheet(self._wide_df(), "producao_mil_ton")
        assert set(df.columns) == {"uf", "safra", "producao_mil_ton"}
