import asyncio
import io
import json
import os

import boto3
import moto
import pytest
import requests
from botocore.exceptions import ClientError
from tornado.httpclient import AsyncHTTPClient, HTTPClientError
from tornado.testing import AsyncHTTPTestCase, gen_test

import app
from tests.fixtures import (
    StopLoopException, local_file, s3_mocked, broker_stage_messages, event_loop,
    produce_queue_mocked
) # flake8: noqa
from tests.fixtures.fake_mq import FakeMQ
from utils.storage import s3 as s3_storage

client = AsyncHTTPClient()
with open('VERSION', 'rb') as f:
    VERSION = f.read()


class TestStatusHandler(AsyncHTTPTestCase):
    def get_app(self):
        return app.app

    @gen_test
    def test_check_everything_up(self):
        with FakeMQ():
            with moto.mock_s3():
                s3_storage.s3 = boto3.client('s3')
                s3_storage.s3.create_bucket(Bucket=s3_storage.QUARANTINE)
                s3_storage.s3.create_bucket(Bucket=s3_storage.PERM)
                s3_storage.s3.create_bucket(Bucket=s3_storage.REJECT)

                self.io_loop.spawn_callback(app.producer)
                self.io_loop.spawn_callback(app.consumer)

                response = yield self.http_client.fetch(self.get_url('/api/v1/status'), method='GET')

                body = json.loads(response.body)

                self.assertDictEqual(
                    body,
                    {
                        "upload_service": "up",
                        "message_queue_consumer": "up",
                        "message_queue_producer": "up",
                        "long_term_storage": "up",
                        "quarantine_storage": "up",
                        "rejected_storage": "up"
                    }
                )

    @gen_test
    def test_check_everything_down(self):
        with FakeMQ(is_down=True):
            with moto.mock_s3():
                s3_storage.s3 = boto3.client('s3')
                response = yield self.http_client.fetch(self.get_url('/api/v1/status'), method='GET')

                body = json.loads(response.body)

                self.assertDictEqual(
                    body,
                    {
                        "upload_service": "up",
                        "message_queue_producer": "down",
                        "message_queue_consumer": "down",
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


class TestProducerAndConsumer:

    @staticmethod
    def _create_message_s3(_file, _stage_message, avoid_produce_queue=False, validation='success',
                           topic='platform.upload.validation'):
        return _stage_message(_file, topic, avoid_produce_queue, validation)

    @asyncio.coroutine
    async def coroutine_test(self, method, exc_message='Stopping the iteration'):
        with pytest.raises(StopLoopException, message=exc_message) as e:
            await method()

        assert str(e.value) == exc_message

    def test_producer_with_s3_bucket(self, local_file, s3_mocked, broker_stage_messages, event_loop):
        total_messages = 4
        [self._create_message_s3(local_file, broker_stage_messages) for _ in range(total_messages)]

        with FakeMQ():
            assert app.mqp.produce_calls_count == 0
            assert len(app.produce_queue) == total_messages

            event_loop.run_until_complete(self.coroutine_test(app.producer))

            assert app.mqp.produce_calls_count == total_messages
            assert len(app.produce_queue) == 0
            assert app.mqp.disconnect_in_operation_called is False
            assert app.mqp.trying_to_connect_failures_calls == 0

    def test_consumer_with_s3_bucket(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        topic = 'platform.upload.validation'
        produced_messages = []
        with FakeMQ():
            for _ in range(total_messages):
                message = self._create_message_s3(
                    local_file, broker_stage_messages, avoid_produce_queue=True, topic=topic
                )
                app.mqc.send_and_wait(topic, json.dumps(message).encode('utf-8'), True)
                produced_messages.append(message)

            for m in produced_messages:
                assert s3_storage.ls(s3_storage.QUARANTINE, m['payload_id'])['ResponseMetadata']['HTTPStatusCode'] == 200

            assert app.mqc.produce_calls_count == total_messages
            assert app.mqc.count_topic_messages(topic) == total_messages
            assert len(app.produce_queue) == 0
            assert app.mqc.consume_calls_count == 0

            event_loop.run_until_complete(self.coroutine_test(app.consumer))

            for m in produced_messages:
                with pytest.raises(ClientError) as e:
                    s3_storage.ls(s3_storage.QUARANTINE, m['payload_id'])
                assert str(e.value) == 'An error occurred (404) when calling the HeadObject operation: Not Found'

                assert s3_storage.ls(s3_storage.PERM, m['payload_id'])['ResponseMetadata']['HTTPStatusCode'] == 200

            assert app.mqc.consume_calls_count > 0
            assert app.mqc.consume_return_messages_count == 1

            assert app.mqc.count_topic_messages(topic) == 0
            assert app.mqc.disconnect_in_operation_called is False
            assert app.mqc.trying_to_connect_failures_calls == 0
            assert len(app.produce_queue) == 4

    def test_consumer_with_validation_failure(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        topic = 'platform.upload.validation'
        s3_storage.s3.create_bucket(Bucket=s3_storage.REJECT)
        produced_messages = []

        with FakeMQ():
            for _ in range(total_messages):
                message = self._create_message_s3(
                    local_file, broker_stage_messages, avoid_produce_queue=True, topic=topic, validation='failure'
                )
                app.mqc.send_and_wait(topic, json.dumps(message).encode('utf-8'), True)
                produced_messages.append(message)

            assert app.mqc.produce_calls_count == total_messages
            assert app.mqc.count_topic_messages(topic) == total_messages
            assert len(app.produce_queue) == 0
            assert app.mqc.consume_calls_count == 0

            event_loop.run_until_complete(self.coroutine_test(app.consumer))

            for m in produced_messages:
                with pytest.raises(ClientError) as e:
                    s3_storage.ls(s3_storage.QUARANTINE, m['payload_id'])
                assert str(e.value) == 'An error occurred (404) when calling the HeadObject operation: Not Found'

                assert s3_storage.ls(s3_storage.REJECT, m['payload_id'])['ResponseMetadata']['HTTPStatusCode'] == 200

            assert app.mqc.consume_calls_count > 0
            assert app.mqc.consume_return_messages_count == 1

            assert app.mqc.count_topic_messages(topic) == 0
            assert app.mqc.disconnect_in_operation_called is False
            assert app.mqc.trying_to_connect_failures_calls == 0
            assert len(app.produce_queue) == 0

    def test_consumer_with_validation_unknown(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        topic = 'platform.upload.validation'
        s3_storage.s3.create_bucket(Bucket=s3_storage.REJECT)
        produced_messages = []

        with FakeMQ():
            for _ in range(total_messages):
                message = self._create_message_s3(
                    local_file, broker_stage_messages, avoid_produce_queue=True, topic=topic, validation='unknown'
                )
                app.mqc.send_and_wait(topic, json.dumps(message).encode('utf-8'), True)
                produced_messages.append(message)

            assert app.mqc.produce_calls_count == total_messages
            assert app.mqc.count_topic_messages(topic) == total_messages
            assert len(app.produce_queue) == 0
            assert app.mqc.consume_calls_count == 0

            event_loop.run_until_complete(self.coroutine_test(app.consumer))

            assert app.mqc.consume_calls_count > 0
            assert app.mqc.consume_return_messages_count == 1

            assert app.mqc.count_topic_messages(topic) == 0
            assert app.mqc.disconnect_in_operation_called is False
            assert app.mqc.trying_to_connect_failures_calls == 0
            assert len(app.produce_queue) == 0

    def test_producer_with_connection_issues(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        [self._create_message_s3(local_file, broker_stage_messages) for _ in range(total_messages)]

        with FakeMQ(connection_failing_attempt_countdown=1, disconnect_in_operation=2):
            assert app.mqp.produce_calls_count == 0
            assert len(app.produce_queue) == total_messages

            event_loop.run_until_complete(self.coroutine_test(app.producer))

            assert app.mqp.produce_calls_count == total_messages
            assert len(app.produce_queue) == 0
            assert app.mqp.disconnect_in_operation_called is True
            assert app.mqp.trying_to_connect_failures_calls == 1

    def test_consumer_with_connection_issues(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        topic = 'platform.upload.validation'

        with FakeMQ(connection_failing_attempt_countdown=1, disconnect_in_operation=2):
            for _ in range(total_messages):
                message = self._create_message_s3(
                    local_file, broker_stage_messages, avoid_produce_queue=True, topic=topic
                )
                app.mqc.send_and_wait(topic, json.dumps(message).encode('utf-8'), True)

            assert app.mqc.produce_calls_count == total_messages
            assert app.mqc.count_topic_messages(topic) == total_messages
            assert len(app.produce_queue) == 0
            assert app.mqc.consume_calls_count == 0

            event_loop.run_until_complete(self.coroutine_test(app.consumer))

            assert app.mqc.consume_calls_count > 0
            assert app.mqc.consume_return_messages_count == 1

            assert app.mqc.count_topic_messages(topic) == 0
            assert app.mqc.disconnect_in_operation_called is True
            assert app.mqc.trying_to_connect_failures_calls == 1
            assert len(app.produce_queue) == 4
