import pandas as pd
import pytest

from src.utils import get_logger, http_get_json, upload_to_bigquery


class TestGetLogger:
    def test_returns_logger_with_correct_name(self):
        logger = get_logger("test_module")
        assert logger.name == "test_module"

    def test_returns_same_logger_on_repeated_calls(self):
        a = get_logger("same")
        b = get_logger("same")
        assert a is b


class TestHttpGetJson:
    def test_returns_parsed_json(self, mocker):
        mock_response = mocker.MagicMock()
        mock_response.json.return_value = [{"key": "value"}]
        mocker.patch("src.utils.requests.get", return_value=mock_response)

        result = http_get_json("http://fake-url")

        assert result == [{"key": "value"}]

    def test_raises_on_http_error(self, mocker):
        import requests as req

        mock_response = mocker.MagicMock()
        mock_response.raise_for_status.side_effect = req.HTTPError("500")
        mocker.patch("src.utils.requests.get", return_value=mock_response)

        with pytest.raises(req.HTTPError):
            http_get_json("http://bad-url")

    def test_passes_headers_to_request(self, mocker):
        mock_get = mocker.patch("src.utils.requests.get")
        mock_get.return_value.json.return_value = {}

        http_get_json("http://fake-url", headers={"X-Key": "val"})

        call_kwargs = mock_get.call_args
        assert call_kwargs[1]["headers"] == {"X-Key": "val"}

    def test_uses_empty_headers_when_none(self, mocker):
        mock_get = mocker.patch("src.utils.requests.get")
        mock_get.return_value.json.return_value = {}

        http_get_json("http://fake-url")

        assert mock_get.call_args[1]["headers"] == {}


class TestUploadToBigQuery:
    def test_calls_load_table_from_dataframe(self, mocker):
        mock_client_cls = mocker.patch("src.utils.bigquery.Client")
        mock_client = mock_client_cls.return_value
        mock_client.load_table_from_dataframe.return_value.result.return_value = None

        df = pd.DataFrame({"col": [1, 2]})
        upload_to_bigquery(df, "project.dataset.table")

        mock_client.load_table_from_dataframe.assert_called_once()

    def test_uses_write_truncate_disposition(self, mocker):
        from google.cloud import bigquery as bq

        mock_client_cls = mocker.patch("src.utils.bigquery.Client")
        mock_client = mock_client_cls.return_value
        mock_client.load_table_from_dataframe.return_value.result.return_value = None
        mock_job_config = mocker.patch("src.utils.bigquery.LoadJobConfig")

        df = pd.DataFrame({"col": [1]})
        upload_to_bigquery(df, "project.dataset.table")

        mock_job_config.assert_called_once_with(
            write_disposition=bq.WriteDisposition.WRITE_TRUNCATE
        )
