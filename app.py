import tornado.ioloop
import tornado.web
import os
import re
import uuid


content_regex = '^application/vnd\.redhat\.([a-z]+)\.([a-z]+)\+(tgz|zip)$'
# set max length to 10.5 MB (one MB larger than peak)
max_length = os.getenv('MAX_LENGTH', 11010048)
listen_port = os.getenv('LISTEN_PORT', 8888)

file_dict = {}

# all storage below is local for testing. need to decide on a real object store


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
    # accepts uploads. No auth implemented yet

    def get(self):
        self.write("Accepted Content-Types: gzipped tarfile, zip file")

    def post(self):
        invalid = upload_validation(self.request.headers)
        content_type = self.request.headers['Content-type']
        if invalid:
            self.set_status(invalid[0], invalid[1])
        else:
            filename = content_type.split('.')[-1].replace('+', '.')
            service = content_type.split('.')[2]
            hash_value = uuid.uuid4().hex
            with open('/tmp/' + service + '/' + filename, 'w') as f:
                f.write(self.request.body)
                file_dict[hash_value] = '/tmp/' + service + '/' + filename
            self.set_status(202, 'Accepted')
            # printing hash_value for testing. need to use for downloading file
            print(hash_value)

    def options(self):
        self.add_header('Allow', 'GET, POST, HEAD, OPTIONS')


class TmpFileHandler(tornado.web.RequestHandler):
    # temporary location for apps to grab the files from. once validated,
    # they send an empty PUT request to approve it or a DELETE request to
    # remove it. If approved, it moves to a more permanent location with a
    # new URI

    def get(self):
        buf_size = 4096
        with open(file_dict[self.request.uri.split('/')[3]], 'r') as f:
            while True:
                data = f.read(buf_size)
                if not data:
                    self.set_status(404)
                    break
                self.set_status(200, 'OK')
                self.write(data)
        self.finish()

    def put(self):
        new_path = "/tmp/new_dir/" + file_dict[self.request.uri.split('/')[3]].split('/')[-1]
        os.rename(file_dict[self.request.uri.split('/')[3]], new_path)
        hash_value = uuid.uuid4().hex
        file_dict[hash_value] = new_path
        self.set_status(204, 'No Content')
        self.add_header('Package-URI', "/v1/store/" + hash_value)

    def delete(self):
        self.set_status(202, 'Accepted')
        os.remove(file_dict[self.request.uri.split('/')[3]])
        file_dict.pop(self.request.uri.split('/')[3], none)


class StaticFileHandler(tornado.web.RequestHandler):
    # Location for grabbing file from the long term storage

    def get(self):
        self.write('booop placeholder')


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/v1/upload", UploadHandler),
        (r"/v1/tmpstore/\w+", TmpFileHandler),
        (r"/v1/store/\w+", StaticFileHandler)
    ])


if __name__ == "__main__":
    app = make_app()
    app.listen(listen_port)
    tornado.ioloop.IOLoop.current().start()
