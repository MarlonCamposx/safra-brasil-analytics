import pandas as pd
import pytest

from src.ingest_inmet import (
    TABLE_ID,
    TABLE_ID_MEASUREMENTS,
    _to_float,
    fetch_measurements,
    fetch_stations,
    main,
    normalize_measurements,
    normalize_stations,
)


class TestToFloat:
    """Testa o helper de conversão para float."""

    def test_converts_string_number(self):
        assert _to_float("3.14") == pytest.approx(3.14)

    def test_converts_comma_decimal(self):
        assert _to_float("-15,789") == pytest.approx(-15.789)

    def test_returns_none_for_none(self):
        assert _to_float(None) is None

    def test_returns_none_for_invalid_string(self):
        assert _to_float("N/A") is None

    def test_converts_int(self):
        assert _to_float(100) == 100.0


class TestNormalizeStations:
    """Testa a normalização dos registros brutos da API INMET."""

    def _api_records(self):
        return [
            {
                "CD_ESTACAO": "A001",
                "DC_NOME": "BRASILIA",
                "SG_ESTADO": "DF",
                "TP_ESTACAO": "Automatica",
                "VL_LATITUDE": "-15.789444",
                "VL_LONGITUDE": "-47.925833",
                "VL_ALTITUDE": "1160.0",
                "DT_INICIO_OPERACAO": "2000-05-13",
                "DT_FIM_OPERACAO": None,
                "CD_SITUACAO": "Operante",
                "FL_CAPITAL": "Y",
                "CD_OSCAR": "0-20000-0-86716",
                "CD_DISTRITO": "5",
                "SG_ENTIDADE": "INMET",
                "CD_WSI": "0-76-0-A001",
            },
            {
                "CD_ESTACAO": "B001",
                "DC_NOME": "SAO PAULO - MIRANTE",
                "SG_ESTADO": "SP",
                "TP_ESTACAO": "Convencional",
                "VL_LATITUDE": "-23.496389",
                "VL_LONGITUDE": "-46.620278",
                "VL_ALTITUDE": "786.0",
                "DT_INICIO_OPERACAO": "1943-01-01",
                "DT_FIM_OPERACAO": None,
                "CD_SITUACAO": "Operante",
                "FL_CAPITAL": "N",
                "CD_OSCAR": None,
                "CD_DISTRITO": "2",
                "SG_ENTIDADE": "INMET",
                "CD_WSI": None,
            },
        ]

    def test_empty_records_returns_empty_dataframe(self):
        df = normalize_stations([])
        assert df.empty

    def test_returns_correct_row_count(self):
        df = normalize_stations(self._api_records())
        assert len(df) == 2

    def test_maps_station_code(self):
        df = normalize_stations(self._api_records())
        assert "A001" in df["cd_estacao"].values

    def test_converts_latitude_to_float(self):
        df = normalize_stations(self._api_records())
        row = df[df["cd_estacao"] == "A001"].iloc[0]
        assert isinstance(row["vl_latitude"], float)
        assert row["vl_latitude"] == pytest.approx(-15.789444)

    def test_none_end_date_preserved(self):
        df = normalize_stations(self._api_records())
        assert df["dt_fim_operacao"].isna().any()

    def test_output_has_expected_columns(self):
        df = normalize_stations(self._api_records())
        expected = {
            "cd_estacao",
            "dc_nome",
            "sg_estado",
            "tp_estacao",
            "vl_latitude",
            "vl_longitude",
            "vl_altitude",
            "dt_inicio_operacao",
            "dt_fim_operacao",
            "cd_situacao",
            "fl_capital",
            "cd_oscar",
            "cd_distrito",
            "sg_entidade",
            "cd_wsi",
        }
        assert expected.issubset(set(df.columns))


class TestFetchStations:
    """Testa a chamada à API INMET — sem chamadas reais de rede."""

    def test_calls_correct_url_for_automatic(self, mocker):
        mock_http = mocker.patch(
            "src.ingest_inmet.http_get_json",
            return_value=[{"CD_ESTACAO": "A001"}],
        )
        fetch_stations("T")
        url_called = mock_http.call_args[0][0]
        assert url_called.endswith("/estacoes/T")

    def test_calls_correct_url_for_manual(self, mocker):
        mock_http = mocker.patch(
            "src.ingest_inmet.http_get_json",
            return_value=[{"CD_ESTACAO": "B001"}],
        )
        fetch_stations("M")
        url_called = mock_http.call_args[0][0]
        assert url_called.endswith("/estacoes/M")

    def test_returns_raw_records(self, mocker):
        fake = [{"CD_ESTACAO": "A001"}, {"CD_ESTACAO": "A002"}]
        mocker.patch("src.ingest_inmet.http_get_json", return_value=fake)
        result = fetch_stations("T")
        assert result == fake


class TestNormalizeMeasurements:
    """Testa a normalização dos registros de medição da API INMET."""

    def _api_records(self):
        return [
            {
                "CD_ESTACAO": "A001",
                "DT_MEDICAO": "2022-07-10",
                "CHUVA": "12.4",
            },
            {
                "CD_ESTACAO": "A001",
                "DT_MEDICAO": "2022-07-11",
                "CHUVA": None,
            },
        ]

    def test_empty_records_returns_empty_dataframe(self):
        df = normalize_measurements([])
        assert df.empty

    def test_returns_correct_row_count(self):
        df = normalize_measurements(self._api_records())
        assert len(df) == 2

    def test_output_has_expected_columns(self):
        df = normalize_measurements(self._api_records())
        expected = {"cd_estacao", "measurement_date", "precip_mm"}
        assert expected.issubset(set(df.columns))

    def test_converts_precip_to_float(self):
        df = normalize_measurements(self._api_records())
        row = df[df["measurement_date"] == "2022-07-10"].iloc[0]
        assert isinstance(row["precip_mm"], float)
        assert row["precip_mm"] == pytest.approx(12.4)

    def test_none_precip_preserved_as_null(self):
        df = normalize_measurements(self._api_records())
        assert df["precip_mm"].isna().any()

    def test_maps_station_code(self):
        df = normalize_measurements(self._api_records())
        assert "A001" in df["cd_estacao"].values


class TestFetchMeasurements:
    """Testa a chamada ao endpoint de medições históricas — sem chamadas reais de rede."""

    def test_calls_correct_url(self, mocker):
        mock_http = mocker.patch(
            "src.ingest_inmet.http_get_json",
            return_value=[{"CD_ESTACAO": "A001"}],
        )
        fetch_measurements("A001", "2022-01-01", "2022-12-31")
        url_called = mock_http.call_args[0][0]
        assert "historico" in url_called
        assert "A001" in url_called
        assert "2022-01-01" in url_called
        assert "2022-12-31" in url_called

    def test_returns_raw_records(self, mocker):
        fake = [{"CD_ESTACAO": "A001", "DT_MEDICAO": "2022-07-10", "CHUVA": "5.0"}]
        mocker.patch("src.ingest_inmet.http_get_json", return_value=fake)
        result = fetch_measurements("A001", "2022-01-01", "2022-12-31")
        assert result == fake

    def test_returns_empty_list_when_api_returns_nothing(self, mocker):
        mocker.patch("src.ingest_inmet.http_get_json", return_value=[])
        result = fetch_measurements("A001", "2022-01-01", "2022-12-31")
        assert result == []


class TestMain:
    """Testa a orquestração do pipeline de ingestão INMET."""

    def _fake_station_df(self):
        return pd.DataFrame(
            [
                {
                    "cd_estacao": "A001",
                    "dc_nome": "BRASILIA",
                    "sg_estado": "DF",
                    "tp_estacao": "Automatica",
                    "vl_latitude": -15.79,
                    "vl_longitude": -47.93,
                    "vl_altitude": 1160.0,
                    "dt_inicio_operacao": "2000-05-13",
                    "dt_fim_operacao": None,
                    "cd_situacao": "Operante",
                    "fl_capital": "Y",
                    "cd_oscar": None,
                    "cd_distrito": "5",
                    "sg_entidade": "INMET",
                    "cd_wsi": None,
                }
            ]
        )

    def test_uploads_stations_and_measurements(self, mocker):
        fake_station_df = self._fake_station_df()
        fake_measurement_df = pd.DataFrame(
            [
                {
                    "cd_estacao": "A001",
                    "measurement_date": "2022-07-10",
                    "precip_mm": 12.4,
                }
            ]
        )

        mocker.patch("src.ingest_inmet.fetch_stations", return_value=[])
        mocker.patch("src.ingest_inmet.normalize_stations", return_value=fake_station_df)
        mocker.patch("src.ingest_inmet.fetch_measurements", return_value=[{}])
        mocker.patch("src.ingest_inmet.normalize_measurements", return_value=fake_measurement_df)
        mock_upload = mocker.patch("src.ingest_inmet.upload_to_bigquery")

        main()

        assert mock_upload.call_count == 2
        uploaded_tables = [call[0][1] for call in mock_upload.call_args_list]
        assert TABLE_ID in uploaded_tables
        assert TABLE_ID_MEASUREMENTS in uploaded_tables

    def test_skips_measurement_upload_when_none_returned(self, mocker):
        fake_station_df = self._fake_station_df()

        mocker.patch("src.ingest_inmet.fetch_stations", return_value=[])
        mocker.patch("src.ingest_inmet.normalize_stations", return_value=fake_station_df)
        mocker.patch("src.ingest_inmet.fetch_measurements", return_value=[])
        mocker.patch("src.ingest_inmet.normalize_measurements", return_value=pd.DataFrame())
        mock_upload = mocker.patch("src.ingest_inmet.upload_to_bigquery")

        main()

        assert mock_upload.call_count == 1
        assert mock_upload.call_args[0][1] == TABLE_ID
