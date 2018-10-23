from app import app, UploadHandler
from importlib import import_module
from mock.mock import Mock, patch
from tempfile import NamedTemporaryFile
from tornado.testing import AsyncHTTPTestCase, gen_test


localdisk = import_module("utils.storage.localdisk")


async def process_upload(filename, size, tracking_id, payload_id, identity, service):
    """
    A dummy method to path the real process_upload method. It must be asynchronous, but (Magic)Mock doesn’t support
    that.
    """
    pass


class Anything(object):
    def __eq__(self, other):
        return True


class TestUpload(AsyncHTTPTestCase):
    def get_app(self):
        """
        Get the tornado.web.Application instance.
        """
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

        yield handler.upload("some filename", "some tracking id", "some payload_id")

        logger.exception.assert_not_called()


class TestPost(AsyncHTTPTestCase):

    def get_app(self):
        """
        Get the tornado.web.Application instance.
        """
        return app

    @patch("app.UploadHandler.process_upload", wraps=process_upload)  # Must be an async method.
    @patch("app.UploadHandler.write_data", return_value="some_file.tgz")
    @patch("app.UploadHandler.upload_validation", return_value=False)
    @gen_test
    def test_indentity_default_value(self, upload_validation, write_data, process_upload):
        size = 123
        request = Mock(**{"files": {"upload": [{"content_type": "application/vnd.redhat.testareno.something+tgz",
                                                "body": ""}]},
                          "headers": {"Content-Length": size}})
        handler = UploadHandler(app, request)

        yield handler.post()

        process_upload.assert_called_once_with(Anything(), Anything(), Anything(), Anything(), None, Anything())
