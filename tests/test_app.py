import asyncio
import io
import json
import os

import pytest
import requests
import responses
from tornado.httpclient import AsyncHTTPClient, HTTPClientError
from tornado.testing import AsyncHTTPTestCase, gen_test
from unittest import TestCase
from re import search
from kafkahelpers import ReconnectingClient
from requests import ConnectionError

import app
from tests.fixtures.fake_mq import FakeMQ
from tests.fixtures import StopLoopException
from utils.storage import s3 as s3_storage
from mock import patch

client = AsyncHTTPClient()
with open('VERSION', 'rb') as f:
    VERSION = f.read()


class TestContentRegex(TestCase):
    """
    Test the content MIME type regex described in IPP 1.
    """

    def test_valid_mime_type(self):
        """
        A valid MIME type is correctly recognized.
        """
        mime_types = [
            'application/vnd.redhat.insights.advisor+zip',
            'application/vnd.redhat.insights.compliance+tgz',
            'application/vnd.redhat.my-app.service1+zip',
            'application/vnd.redhat.s0m3-s3rv1c3.s0m3-4pp+tgz'
        ]
        for mime_type in mime_types:
            with self.subTest(mime_type=mime_type):
                self.assertIsNotNone(search(app.content_regex, mime_type))

    def test_invalid_mime_type(self):
        """
        An invalid MIME type is correctly recognized.
        """
        mime_types = [
            'application/vnd.redhat.insights.advisor+tbz2',
            'application/vnd.redhat.compliance+tgz',
            'application/vnd.redhat.my_app.service+zip',
            'text/vnd.redhat.insights.advisor+tgz',
            'application/bbq.redhat.insights.advisor+tgz',
            'application/vnd.ibm.insights.advisor+tgz'
        ]
        for mime_type in mime_types:
            with self.subTest(mime_type=mime_type):
                self.assertIsNone(search(app.content_regex, mime_type))

    def test_supports_legacy(self):
        self.assertEqual("advisor", app.get_service("application/x-gzip; charset=binary"))
        self.assertEqual("fab", app.get_service("application/vnd.redhat.fab.service+tgz"))


class TestUploadHandler(AsyncHTTPTestCase):

    @staticmethod
    def prepare_request_context(file_size=100, file_name=None, mime_type='application/vnd.redhat.advisor.payload+tgz',
                                file_field_name='upload'):

        # Build HTTP Request so that Tornado can recognize and use the payload test
        request = requests.Request(
            url="http://localhost:8888/r/insights/platform/upload/api/v1/upload", data={},
            files={file_field_name: (file_name, io.BytesIO(os.urandom(file_size)), mime_type)} if file_name else None,
        )
        request.headers["x-rh-insights-request-id"] = "test"

        return request.prepare()

    def get_app(self):
        return app.app

    @gen_test
    def test_root_get(self):
        response = yield self.http_client.fetch(self.get_url('/r/insights/platform/upload'), method='GET')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b'boop')
        response = yield self.http_client.fetch(self.get_url('/r/insights/platform/upload'), method='OPTIONS')
        self.assertEqual(response.headers['Allow'], 'GET, HEAD, OPTIONS')

    @gen_test
    def test_upload_get(self):
        response = yield self.http_client.fetch(self.get_url('/r/insights/platform/upload/api/v1/upload'), method='GET')
        self.assertEqual(response.body, b"Accepted Content-Types: gzipped tarfile, zip file")

    @gen_test
    def test_upload_allowed_methods(self):
        response = yield self.http_client.fetch(self.get_url('/r/insights/platform/upload/api/v1/upload'), method='OPTIONS')
        self.assertEqual(response.headers['Allow'], 'GET, POST, HEAD, OPTIONS')

    @gen_test
    def test_upload_post(self):
        request_context = self.prepare_request_context(100, 'payload.tar.gz')
        response = yield self.http_client.fetch(
            self.get_url('/r/insights/platform/upload/api/v1/upload'),
            method='POST',
            body=request_context.body,
            headers=request_context.headers
        )

        self.assertEqual(response.code, 202)

    @gen_test
    def test_version(self):
        response = yield self.http_client.fetch(self.get_url('/r/insights/platform/upload/api/v1/version'), method='GET')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b'{"version": "%s"}' % VERSION)

    @gen_test
    def test_upload_post_file_too_large(self):
        request_context = self.prepare_request_context(app.MAX_LENGTH + 1, 'payload.tar.gz')

        with self.assertRaises(HTTPClientError) as response:
            yield self.http_client.fetch(
                self.get_url('/r/insights/platform/upload/api/v1/upload'),
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
                self.get_url('/r/insights/platform/upload/api/v1/upload'),
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
                self.get_url('/r/insights/platform/upload/api/v1/upload'),
                method='POST',
                body=request_context.body,
                headers=request_context.headers
            )

        self.assertEqual(response.exception.code, 415)
        self.assertEqual(response.exception.message, 'Upload field not found')


class TestInventoryPost(object):

    @responses.activate
    @patch("app.INVENTORY_URL", "http://fakeinventory.com/r/insights/platform/inventory/api/v1/hosts")
    def test_post_to_inventory_success(self):
        values = {"account": "12345", "metadata": {"some_key": "some_value"}}
        responses.add(responses.POST, app.INVENTORY_URL,
                      json={"id": "4f81c749-e6e6-46a7-ba3f-e755001ba5ee"}, status=200)
        method_response = app.post_to_inventory('1234', 'abcd1234', values)

        assert method_response is 200
        assert len(responses.calls) == 1
        assert responses.calls[0].response.text == '{"id": "4f81c749-e6e6-46a7-ba3f-e755001ba5ee"}'

    @responses.activate
    @patch("app.INVENTORY_URL", "http://fakeinventory.com/r/insights/platform/inventory/api/v1/hosts")
    def test_post_to_inventory_fail(self):
        values = {"account": "12345", "metadata": {"bad_key": "bad_value"}}
        responses.add(responses.POST, app.INVENTORY_URL,
                      json={"error message": "boop"}, status=400)
        method_response = app.post_to_inventory('1234', 'abcd1234', values)

        assert method_response is 400
        assert len(responses.calls) == 1
        assert responses.calls[0].response.text == '{"error message": "boop"}'


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

        with FakeMQ() as mq:
            client = ReconnectingClient(mq, "producer")
            producer = client.get_callback(app.make_preprocessor())
            assert mq.produce_calls_count == 0
            assert len(app.produce_queue) == total_messages

            event_loop.run_until_complete(self.coroutine_test(producer))

            assert mq.produce_calls_count == total_messages
            assert len(app.produce_queue) == 0
            assert mq.disconnect_in_operation_called is False
            assert mq.trying_to_connect_failures_calls == 0

    def test_consumer_with_s3_bucket(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        topic = 'platform.upload.validation'
        produced_messages = []
        with FakeMQ() as mq:
            client = ReconnectingClient(mq, "consumer")
            consumer = client.get_callback(app.handle_validation)

            for _ in range(total_messages):
                message = self._create_message_s3(
                    local_file, broker_stage_messages, avoid_produce_queue=True, topic=topic
                )
                mq.send_and_wait(topic, json.dumps(message).encode('utf-8'), True)
                produced_messages.append(message)

            for m in produced_messages:
                assert s3_storage.ls(s3_storage.QUARANTINE, m['payload_id'])['ResponseMetadata']['HTTPStatusCode'] == 200

            assert mq.produce_calls_count == total_messages
            assert mq.count_topic_messages(topic) == total_messages
            assert len(app.produce_queue) == 0
            assert mq.consume_calls_count == 0

            event_loop.run_until_complete(self.coroutine_test(consumer))

            for m in produced_messages:
                assert s3_storage.ls(s3_storage.QUARANTINE, m['payload_id'])['ResponseMetadata']['HTTPStatusCode'] == 404
                assert s3_storage.ls(s3_storage.PERM, m['payload_id'])['ResponseMetadata']['HTTPStatusCode'] == 200

            assert mq.consume_calls_count > 0
            assert mq.consume_return_messages_count == 1

            assert mq.count_topic_messages(topic) == 0
            assert mq.disconnect_in_operation_called is False
            assert mq.trying_to_connect_failures_calls == 0
            assert len(app.produce_queue) == 4

    def test_consumer_with_validation_failure(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        topic = 'platform.upload.validation'
        s3_storage.s3.create_bucket(Bucket=s3_storage.REJECT)
        produced_messages = []

        with FakeMQ() as mq:
            client = ReconnectingClient(mq, "consumer")
            consumer = client.get_callback(app.handle_validation)
            for _ in range(total_messages):
                message = self._create_message_s3(
                    local_file, broker_stage_messages, avoid_produce_queue=True, topic=topic, validation='failure'
                )
                mq.send_and_wait(topic, json.dumps(message).encode('utf-8'), True)
                produced_messages.append(message)

            assert mq.produce_calls_count == total_messages
            assert mq.count_topic_messages(topic) == total_messages
            assert len(app.produce_queue) == 0
            assert mq.consume_calls_count == 0

            event_loop.run_until_complete(self.coroutine_test(consumer))

            for m in produced_messages:
                assert s3_storage.ls(s3_storage.QUARANTINE, m['payload_id'])['ResponseMetadata']['HTTPStatusCode'] == 404
                assert s3_storage.ls(s3_storage.REJECT, m['payload_id'])['ResponseMetadata']['HTTPStatusCode'] == 200

            assert mq.consume_calls_count > 0
            assert mq.consume_return_messages_count == 1

            assert mq.count_topic_messages(topic) == 0
            assert mq.disconnect_in_operation_called is False
            assert mq.trying_to_connect_failures_calls == 0
            assert len(app.produce_queue) == 0

    def test_consumer_with_validation_unknown(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        topic = 'platform.upload.validation'
        s3_storage.s3.create_bucket(Bucket=s3_storage.REJECT)
        produced_messages = []

        with FakeMQ() as mq:
            client = ReconnectingClient(mq, "consumer")
            consumer = client.get_callback(app.handle_validation)
            for _ in range(total_messages):
                message = self._create_message_s3(
                    local_file, broker_stage_messages, avoid_produce_queue=True, topic=topic, validation='unknown'
                )
                mq.send_and_wait(topic, json.dumps(message).encode('utf-8'), True)
                produced_messages.append(message)

            assert mq.produce_calls_count == total_messages
            assert mq.count_topic_messages(topic) == total_messages
            assert len(app.produce_queue) == 0
            assert mq.consume_calls_count == 0

            event_loop.run_until_complete(self.coroutine_test(consumer))

            assert mq.consume_calls_count > 0
            assert mq.consume_return_messages_count == 1

            assert mq.count_topic_messages(topic) == 0
            assert mq.disconnect_in_operation_called is False
            assert mq.trying_to_connect_failures_calls == 0
            assert len(app.produce_queue) == 0

    @patch("app.RETRY_INTERVAL", 0.01)
    def test_producer_with_connection_issues(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        [self._create_message_s3(local_file, broker_stage_messages) for _ in range(total_messages)]

        with FakeMQ(connection_failing_attempt_countdown=1, disconnect_in_operation=2) as mq:
            client = ReconnectingClient(mq, "producer")
            producer = client.get_callback(app.make_preprocessor())
            assert mq.produce_calls_count == 0
            assert len(app.produce_queue) == total_messages

            event_loop.run_until_complete(self.coroutine_test(producer))

            assert mq.produce_calls_count == total_messages
            assert len(app.produce_queue) == 0
            assert mq.disconnect_in_operation_called is True
            assert mq.trying_to_connect_failures_calls == 1

    @patch("app.RETRY_INTERVAL", 0.01)
    def test_consumer_with_connection_issues(self, local_file, s3_mocked, broker_stage_messages, event_loop):

        total_messages = 4
        topic = 'platform.upload.validation'

        with FakeMQ(connection_failing_attempt_countdown=1, disconnect_in_operation=2) as mq:
            client = ReconnectingClient(mq, "consumer")
            consumer = client.get_callback(app.handle_validation)
            for _ in range(total_messages):
                message = self._create_message_s3(
                    local_file, broker_stage_messages, avoid_produce_queue=True, topic=topic
                )
                mq.send_and_wait(topic, json.dumps(message).encode('utf-8'), True)

            assert mq.produce_calls_count == total_messages
            assert mq.count_topic_messages(topic) == total_messages
            assert len(app.produce_queue) == 0
            assert mq.consume_calls_count == 0

            event_loop.run_until_complete(self.coroutine_test(consumer))

            assert mq.consume_calls_count > 0
            assert mq.consume_return_messages_count == 1

            assert mq.count_topic_messages(topic) == 0
            assert mq.disconnect_in_operation_called is True
            assert mq.trying_to_connect_failures_calls == 1
            assert len(app.produce_queue) == 4
