import tornado.ioloop
import tornado.web
import os
import re
import uuid

from tempfile import NamedTemporaryFile
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor
from time import gmtime, strftime

from utils import storage, db

content_regex = '^application/vnd\.redhat\.([a-z]+)\.([a-z]+)\+(tgz|zip)$'
# set max length to 10.5 MB (one MB larger than peak)
max_length = os.getenv('MAX_LENGTH', 11010048)
listen_port = os.getenv('LISTEN_PORT', 8888)

# Maximum workers for threaded execution
MAX_WORKERS = 10

# writing these values to sqlite for now
# these are dummy values since we can't yet get a principle or rh_account
values = {'principle': 'dumdum',
          'rh_account': '123456'}

# TODO: create an actual persistent store for id status
status = {}


def split_content(content):
    service = content.split('.')[2]
    filename = content.split('.')[-1]
    return service, filename


def service_notify(payload):
    # Report the new upload to the proper message queue to be ingested by
    # targeted service
    return 'boop'


class RootHandler(tornado.web.RequestHandler):

    def get(self):
        self.write("boop")

    def options(self):
        self.add_header('Allow', 'GET, HEAD, OPTIONS')


class UploadHandler(tornado.web.RequestHandler):
    # accepts uploads. No auth implemented yet, likely to be handled by 3scale
    # anyway.

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    def upload_validation(self):
        if int(self.request.headers['Content-Length']) >= max_length:
            error = (413, 'Payload too large: ' + self.request.headers['Content-Length'])
            return error
        if re.search(content_regex, self.request.files['upload'][0]['content_type']) is None:
            error = (415, 'Unsupported Media Type')
            return error

    def get(self):
        self.write("Accepted Content-Types: gzipped tarfile, zip file")

    @run_on_executor
    def write_data(self):
        with NamedTemporaryFile(delete=False) as tmp:
            tmp.write(self.request.files['upload'][0]['body'])
            tmp.flush()
            filename = tmp.name
        status[self.hash_value] = {'upload_status': 'received',
                                   'update_time': strftime("%Y%m%d-%H:%M:%S", gmtime())}
        response = {'header': ('Status-Enpoint', '/api/v1/upload/status?id=' + self.hash_value),
                    'status': (202, 'Accepted')}
        return response, filename

    @run_on_executor
    def upload(self, filename):
        storage.upload_to_s3(filename, 'insights-upload-quarantine', self.hash_value)

    @tornado.gen.coroutine
    def post(self):
        invalid = self.upload_validation()
        if invalid:
            self.set_status(invalid[0], invalid[1])
        else:
            service, filename = split_content(self.request.files['upload'][0]['content_type'])
            self.hash_value = uuid.uuid4().hex
            result = yield self.write_data()
            self.set_status(result[0]['status'][0], result[0]['status'][1])
            self.set_header(result[0]['header'][0], result[0]['header'][1])
            self.write(status[self.hash_value])
            self.finish()
            self.upload(result[1])
            values['hash'] = self.hash_value
            db.write_to_db(values)

    def options(self):
        self.add_header('Allow', 'GET, POST, HEAD, OPTIONS')


class UploadStatus(tornado.web.RequestHandler):
    # endpoint for getting information on an individual upload.
    # Was upload rejected or eccepted by service

    def get(self):
        upload_id = self.get_argument('id')
        if upload_id == []:
            self.set_status(400)
            return self.finish("Invalid ID")
        self.write(status[upload_id])
        self.finish


class TmpFileHandler(tornado.web.RequestHandler):
    # temporary location for apps to grab the files from. once validated,
    # they send an empty PUT request to approve it or a DELETE request to
    # remove it. If approved, it moves to a more permanent location with a
    # new URI

    def __init__(self):

        self.hash_value = self.request.uri.split('/')[4]

    def get(self):
        buf_size = 4096
        with open(file_dict[self.hash_value], 'r') as f:
            while True:
                data = f.read(buf_size)
                if not data:
                    self.set_status(404)
                    break
                d = {'update_status': 'validating',
                     'update_time': strftime("%Y%m%d-%H:%M:%S", gmtime())}
                self.status['hash_value'].update(d)
                self.set_status(200, 'OK')
                self.write(data)
        self.finish()

    def put(self):
        new_path = "/tmp/new_dir/" + file_dict[self.hash_value].split('/')[-1]
        os.rename(file_dict[self.hash_value], new_path)
        file_dict[self.hash_value] = new_path
        d = {'update_status': 'accepted',
             'update_time': strftime("%Y%m%d-%H:%M:%S", gmtime())}
        status[self.hash_value].update(d)
        self.set_status(204, 'No Content')
        self.add_header('Package-URI', "/v1/store/" + self.hash_value)

    def delete(self):
        d = {'update_status': 'rejected',
             'update_time': strftime("%Y%m%d-%H:%M:%S", gmtime())}
        status[self.hash_value].update(d)
        self.set_status(202, 'Accepted')
        os.remove(file_dict[self.hash_value])
        file_dict.pop(self.hash_value, none)


class StaticFileHandler(tornado.web.RequestHandler):
    # Location for grabbing file from the long term storage

    def get(self):
        # use the storage broker service to grab the file from S3
        self.write('booop placeholder')


class VersionHandler(tornado.web.RequestHandler):

    def get(self):
        response = {'version': '0.0.1'}
        self.write(response)


endpoints = [
    (r"/", RootHandler),
    (r"/api/v1/version", VersionHandler),
    (r"/api/v1/upload", UploadHandler),
    (r"/api/v1/upload/status", UploadStatus),
    (r"/api/v1/tmpstore/\w{32}", TmpFileHandler),
    (r"/api/v1/store/\w{32}", StaticFileHandler)
]

app = tornado.web.Application(endpoints)


if __name__ == "__main__":
    db.createdb(str(db.db_path))
    app.listen(listen_port)
    tornado.ioloop.IOLoop.current().start()
