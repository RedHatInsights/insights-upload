import tornado.ioloop
import tornado.web
import os
import re


content_regex = '^application/vnd\.redhat\.([a-z]+)\.([a-z]+)\+(tgz|zip)$'
# set max length to 10.5 MB
max_length = os.getenv('MAX_LENGTH', 11010048)
listen_port = os.getenv('LISTEN_PORT', 8888)


def upload_validation(upload):
    if int(upload['Content-Length']) >= max_length:
        code, msg = 413, 'Payload too large: ' + upload['Content-Length']
        return code, msg
    if re.search(content_regex, upload['Content-type']) is None:
        code, msg = 415, 'Unsupported Media Type'
        return code, msg


class MainHandler(tornado.web.RequestHandler):

    def get(self):
        self.write("boop")

    def options(self):
        self.add_header('Allow', 'GET, HEAD, OPTIONS')

class UploadHandler(tornado.web.RequestHandler):

    def get(self):
        self.write("Accepted Content-Types: gzipped tarfile, zip file")

    def post(self):
        invalid = upload_validation(self.request.headers)
        if invalid:
            self.set_status(invalid[0], invalid[1])
        else:
            with open('test-archive.tar.gz', 'w') as f:
                f.write(self.request.body)
            self.set_status(202, 'Accepted')

    def options(self):
        self.add_header('Allow', 'GET, POST, HEAD, OPTIONS')


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/v1/upload", UploadHandler),
    ])


if __name__ == "__main__":
    app = make_app()
    app.listen(listen_port)
    tornado.ioloop.IOLoop.current().start()
