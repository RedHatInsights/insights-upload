from tests.fixtures import *
from utils import mnm
import responses


class TestInfluxDB:

    @responses.activate
    def test_send_to_influxdb(self, influx_db_mock, influx_db_credentials, influx_db_values):
        method_response = mnm.send_to_influxdb(influx_db_values)

        assert method_response is None
        assert len(responses.calls) == 1
        assert responses.calls[0].request.url == influx_db_mock
        assert responses.calls[0].response.text == '{"message": "saved"}'

    @responses.activate
    def test_send_to_influxdb_no_credentials(self, influx_db_mock, influx_db_values):
        method_response = mnm.send_to_influxdb(influx_db_values)

        assert method_response is None
        assert len(responses.calls) == 0

    @responses.activate
    def test_send_to_influxdb_down(self, influx_db_error_mock, influx_db_credentials, influx_db_values):
        method_response = mnm.send_to_influxdb(influx_db_values)
        assert method_response is None
        assert len(responses.calls) == 1
        assert responses.calls[0].request.url == influx_db_error_mock
        assert responses.calls[0].response.text == '{"message": "error"}'

    def test_namespace(self, influx_db_namespace):
        assert mnm.NAMESPACE == 'somenamespace'
