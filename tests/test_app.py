import unittest
import requests

from tornado.httpclient import AsyncHTTPClient
from tornado.testing import AsyncTestCase, gen_test

import app

URL = "http://upload-service-platform-ci.1b13.insights.openshiftapps.com"

client = AsyncHTTPClient()

class TestEndpoints(AsyncTestCase):
    @gen_test
    def test_root_get(self):
        response = yield client.fetch(URL, method='GET')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b'boop')
        response = yield client.fetch(URL, method='OPTIONS')
        self.assertEqual(response.headers['Allow'], 'GET, HEAD, OPTIONS')

    @gen_test
    def test_upload(self):
        response = yield client.fetch(URL + '/api/v1/upload', method='GET')
        self.assertEqual(response.body, b"Accepted Content-Types: gzipped tarfile, zip file")
        response = yield client.fetch(URL + '/api/v1/upload', method='OPTIONS')
        self.assertEqual(response.headers['Allow'], 'GET, POST, HEAD, OPTIONS')

    @gen_test
    def test_version(self):
        response = yield client.fetch(URL + '/api/v1/version', method='GET')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b'{"version": "0.0.1"}')
