import requests

from tornado.httpclient import AsyncHTTPClient
from tornado.testing import AsyncHTTPTestCase, gen_test

import app

client = AsyncHTTPClient()

# Build HTTP Request so that Tornado can recognize and use the payload test
files = {"upload": ('payload.tar.gz', open('./tests/payload.tar.gz', 'rb'),
         'application/vnd.redhat.advisor.payload+tgz')}
data = {}
a = requests.Request(url="http://localhost:8888/api/v1/upload",
                     files=files, data=data)
prepare = a.prepare()
content_type = prepare.headers.get('Content-Type')
body = prepare.body
headers = {"Content-Type": content_type}


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
        response = yield self.http_client.fetch(self.get_url('/api/v1/upload'), method='POST', body=body, headers=headers)
        self.assertEqual(response.code, 202)

    @gen_test
    def test_version(self):
        response = yield self.http_client.fetch(self.get_url('/api/v1/version'), method='GET')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b'{"version": "0.0.1"}')
