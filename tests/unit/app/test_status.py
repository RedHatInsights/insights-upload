import json
import moto
import boto3
import mock
from tornado.gen import coroutine
from tornado.httpclient import AsyncHTTPClient
from tornado.testing import AsyncHTTPTestCase, gen_test

import app

from utils.storage import s3 as storage

client = AsyncHTTPClient()

with open('VERSION', 'rb') as f:
    VERSION = f.read()


def mqc_connect_mock():
    conn_result = mock.Mock(name="CoroutineResult")
    conn_func = mock.Mock(name="CoroutineFunction", side_effect=coroutine(conn_result))
    conn_func.connect = conn_func
    conn_func.connect.return_value = True
    return conn_func


class TestStatusHandler(AsyncHTTPTestCase):

    def get_app(self):
        return app.app

    @gen_test
    @mock.patch('app.mqc', new_callable=mqc_connect_mock)
    def test_check_everything_up(self, mqc_mock):
        app.mqc = mqc_mock

        with moto.mock_s3():
            storage.s3 = boto3.client('s3')
            storage.s3.create_bucket(Bucket=storage.QUARANTINE)
            storage.s3.create_bucket(Bucket=storage.PERM)
            storage.s3.create_bucket(Bucket=storage.REJECT)

            response = yield self.http_client.fetch(self.get_url('/api/v1/status'), method='GET')

            body = json.loads(response.body)

            self.assertDictEqual(
                body,
                {
                    "upload_service": "up",
                     "message_queue": "up",
                     "long_term_storage": "up",
                     "quarantine_storage": "up",
                     "rejected_storage": "up"
                }
            )

    @gen_test
    def test_check_everything_down(self):
        with moto.mock_s3():
            storage.s3 = boto3.client('s3')
            response = yield self.http_client.fetch(self.get_url('/api/v1/status'), method='GET')

            body = json.loads(response.body)

            self.assertDictEqual(
                body,
                {
                    "upload_service": "up",
                    "message_queue": "down",
                    "long_term_storage": "down",
                    "quarantine_storage": "down",
                    "rejected_storage": "down"
                }
            )
