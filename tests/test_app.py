import app
import tornado


class TestApp(tornado.testing.AsyncHTTPTestCase):
    def get_app(self):
        return app.app.listen(8888)

    def test_root_get(self):
        response = self.fetch('/')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, 'boop')

    def test_root_options(self):
        response = self.fetch('/', method='OPTIONS')
        self.assertEqual(response.headers['Allow'], 'GET, HEAD, OPTIONS')

    def test_upload_get(self):
        response = self.fetch('/api/v1/upload')
        self.assertEqual(response.body, 'Accepted Content-Types: gzipped tarfile, zip file')

    def test_upload_options(self):
        response = self.fetch('/api/v1/upload', method='OPTIONS')
        self.assertEqual(response.headers['Allow'], 'GET, POST, HEAD, OPTIONS')

