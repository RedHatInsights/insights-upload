from app import app, UploadHandler
from importlib import import_module
from mock.mock import Mock, patch
from tempfile import NamedTemporaryFile
from tornado.testing import AsyncHTTPTestCase, gen_test


class TestUpload(AsyncHTTPTestCase):
    localdisk = import_module("utils.storage.localdisk")

    def get_app(self):
        return app

    @patch("app.os.remove")
    @patch("app.logger")
    @patch("utils.storage.localdisk.open")
    @patch("utils.storage.localdisk.os.path.isdir", return_value=True)
    @patch("app.storage", localdisk)
    @gen_test
    def test_localdisk_write_does_not_fail(self, isdir, open_mock, logger, remove):
        """
        Calling localdisk.write method does not fail when everything goes right, correct number of values is returned.
        """
        open_mock.return_value = NamedTemporaryFile("w")

        request = Mock()
        handler = UploadHandler(app, request)

        yield handler.upload("some filename", "some tracking id", "some hash value")

        logger.exception.assert_not_called()
