import io
import json
import os

import boto3
import mock
import moto
import requests
from tornado.gen import coroutine
from tornado.httpclient import AsyncHTTPClient, HTTPClientError
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


class TestUploadHandler(AsyncHTTPTestCase):

    @staticmethod
    def prepare_request_context(file_size=100, file_name=None, mime_type='application/vnd.redhat.advisor.payload+tgz',
                                file_field_name='upload'):

        # Build HTTP Request so that Tornado can recognize and use the payload test
        request = requests.Request(
            url="http://localhost:8888/api/v1/upload", data={},
            files={file_field_name: (file_name, io.BytesIO(os.urandom(file_size)), mime_type)} if file_name else None
        )

        return request.prepare()

    def get_app(self):
        return app.app

    @gen_test
    def test_root_get(self):
        response = yield self.http_client.fetch(self.get_url('/'), method='GET')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b'boop')
        response = yield self.http_client.fetch(self.get_url('/'), method='OPTIONS')
        self.assertEqual(response.headers['Allow'], 'GET, HEAD, OPTIONS')

    @gen_test
    def test_upload_get(self):
        response = yield self.http_client.fetch(self.get_url('/api/v1/upload'), method='GET')
        self.assertEqual(response.body, b"Accepted Content-Types: gzipped tarfile, zip file")

    @gen_test
    def test_upload_allowed_methods(self):
        response = yield self.http_client.fetch(self.get_url('/api/v1/upload'), method='OPTIONS')
        self.assertEqual(response.headers['Allow'], 'GET, POST, HEAD, OPTIONS')

    @gen_test
    def test_upload_post(self):
        request_context = self.prepare_request_context(100, 'payload.tar.gz')
        response = yield self.http_client.fetch(
            self.get_url('/api/v1/upload'),
            method='POST',
            body=request_context.body,
            headers=request_context.headers
        )

        self.assertEqual(response.code, 202)

    @gen_test
    def test_version(self):
        response = yield self.http_client.fetch(self.get_url('/api/v1/version'), method='GET')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b'{"version": "%s"}' % VERSION)

    @gen_test
    def test_upload_post_file_too_large(self):
        request_context = self.prepare_request_context(app.MAX_LENGTH + 1, 'payload.tar.gz')

        with self.assertRaises(HTTPClientError) as response:
            yield self.http_client.fetch(
                self.get_url('/api/v1/upload'),
                method='POST',
                body=request_context.body,
                headers=request_context.headers
            )

        self.assertEqual(response.exception.code, 413)
        self.assertEqual(
            'Payload too large: {content_length}. Should not exceed {max_length} bytes'.format(
                content_length=str(request_context.headers.get('Content-Length')),
                max_length=str(app.MAX_LENGTH)
            ), response.exception.message
        )

    @gen_test
    def test_upload_post_file_wrong_mime_type(self):
        request_context = self.prepare_request_context(100, 'payload.tar.gz', mime_type='application/json')

        with self.assertRaises(HTTPClientError) as response:
            yield self.http_client.fetch(
                self.get_url('/api/v1/upload'),
                method='POST',
                body=request_context.body,
                headers=request_context.headers
            )

        self.assertEqual(response.exception.code, 415)
        self.assertEqual(response.exception.message, 'Unsupported Media Type')

    @gen_test
    def test_upload_post_no_file(self):
        request_context = self.prepare_request_context(file_name='payload.tar.gz', file_field_name='not_upload')

        with self.assertRaises(HTTPClientError) as response:
            yield self.http_client.fetch(
                self.get_url('/api/v1/upload'),
                method='POST',
                body=request_context.body,
                headers=request_context.headers
            )

        self.assertEqual(response.exception.code, 415)
        self.assertEqual(response.exception.message, 'Upload field not found')
