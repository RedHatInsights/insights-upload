import unittest
import requests

from tornado.httpclient import AsyncHTTPClient
from tornado.testing import AsyncHTTPTestCase, gen_test

import app

client = AsyncHTTPClient()

class TestEndpoints(AsyncHTTPTestCase):

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
    def test_upload(self):
        response = yield self.http_client.fetch(self.get_url('/api/v1/upload'), method='GET')
        self.assertEqual(response.body, b"Accepted Content-Types: gzipped tarfile, zip file")
        response = yield self.http_client.fetch(self.get_url('/api/v1/upload'), method='OPTIONS')
        self.assertEqual(response.headers['Allow'], 'GET, POST, HEAD, OPTIONS')

    @gen_test
    def test_version(self):
        response = yield self.http_client.fetch(self.get_url('/api/v1/version'), method='GET')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b'{"version": "0.0.1"}')
