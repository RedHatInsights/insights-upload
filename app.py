import tornado.ioloop
import tornado.web
import os
import re
import uuid
import json
import logging

from tempfile import NamedTemporaryFile
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor
from time import gmtime, strftime
from botocore.exceptions import ClientError
from kiel import clients

from utils import storage

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('upload-service')

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


# S3 buckets
quarantine = 'insights-upload-quarantine'
perm = 'insights-upload-perm-test'
reject = 'insights-upload-rejected'


# message queues
mqp = clients.Producer(['kafka.cmitchel-msgq-test.svc:29092'])
mqc = clients.SingleConsumer(brokers=['kafka.cmitchel-msgq-test.svc:29092'])


def split_content(content):
    service = content.split('.')[2]
    filename = content.split('.')[-1]
    return service, filename


def delivery_report(err, msg):
    if err is not None:
        logger.info('Message delivery failed: {}'.format(err))
    else:
        logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


@tornado.gen.coroutine
def consume():
    yield mqc.connect()

    while True:
        msgs = yield mqc.consume('uploadvalidation')
        if msgs:
            handle_file(msgs)


@tornado.gen.coroutine
def handle_file(msgs):

    for msg in msgs:
        hash_ = msg['hash']
        result = msg['validation']

        if result == 'success':
            storage.transfer(hash_, quarantine, perm)
        if result == 'failure':
            logger.info(hash_ + ' rejected')
            storage.transfer(hash_, quarantine, reject)


@tornado.gen.coroutine
def produce(topic, msg):
    yield mqp.connect()
    yield mqp.produce(topic, json.dumps(msg))


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
            error = (413, 'Payload too large: ' + self.request.headers['Content-Length'] + '. Should not exceed ' + max_length + ' bytes')
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
        response = {'header': ('Status-Enpoint', '/api/v1/upload/status?id=' + self.hash_value),
                    'status': (202, 'Accepted')}
        return response, filename

    @run_on_executor
    def upload(self, filename):
        storage.upload_to_s3(filename, quarantine, self.hash_value)
        os.remove(filename)

    @tornado.gen.coroutine
    def post(self):
        if not self.request.files.get('upload'):
            logger.info('Upload field not found')
            self.set_status(415, "Upload field not found")
            self.finish()
        invalid = self.upload_validation()
        if invalid:
            self.set_status(invalid[0], invalid[1])
        else:
            service, filename = split_content(self.request.files['upload'][0]['content_type'])
            self.hash_value = uuid.uuid4().hex
            result = yield self.write_data()
            values['hash'] = self.hash_value
            values['url'] = 'http://upload-service-platform-ci.1b13.insights.openshiftapps.com/api/v1/tmpstore/' + self.hash_value
            self.set_status(result[0]['status'][0], result[0]['status'][1])
            self.set_header(result[0]['header'][0], result[0]['header'][1])
            self.finish()
            self.upload(result[1])
            produce(service, values)

    def options(self):
        self.add_header('Allow', 'GET, POST, HEAD, OPTIONS')


class TmpFileHandler(tornado.web.RequestHandler):
    # temporary location for apps to grab the files from. once validated,
    # they send an empty PUT request to approve it or a DELETE request to
    # remove it. If approved, it moves to a more permanent location with a
    # new URI

    def read_data(self, hash_value):
        with NamedTemporaryFile(delete=False) as tmp:
            filename = tmp.name
            try:
                storage.read_from_s3(quarantine, hash_value, filename)
            except ClientError:
                logger.error('unable to fetch file: %s' % hash_value)
            tmp.flush()
        return filename

    def get(self):
        hash_value = self.request.uri.split('/')[4]
        filename = self.read_data(hash_value)
        buf_size = 4096
        with open(filename, 'rb') as f:
            while True:
                data = f.read(buf_size)
                if not data:
                    self.set_status(404)
                    break
                else:
                    self.set_status(200)
                    self.write(data)
        self.finish()


class StaticFileHandler(tornado.web.RequestHandler):
    # Location for grabbing file from the long term storage

    def read_data(self, hash_value):
        with NamedTemporaryFile(delete=False) as tmp:
            filename = tmp.name
            storage.read_from_s3(perm, hash_value, filename)
            tmp.flush()
        return filename

    def get(self):
        # use the storage broker service to grab the file from S3
        hash_value = self.request.uri.split('/')[4]
        filename = self.read_data(hash_value)
        buf_size = 4096
        with open(filename, 'rb') as f:
            while True:
                data = f.read(buf_size)
                if not data:
                    self.set_status(404)
                    break
                else:
                    self.set_status(200)
                    self.write(data)
        self.finish()

    def delete(self):
        hash_value = self.request.uri.split('/')[4]
        storage.delete_object(hash_value, perm)
        self.set_status(202, 'Accepted')


class VersionHandler(tornado.web.RequestHandler):

    def get(self):
        response = {'version': '0.0.1'}
        self.write(response)


endpoints = [
    (r"/", RootHandler),
    (r"/api/v1/version", VersionHandler),
    (r"/api/v1/upload", UploadHandler),
    (r"/api/v1/tmpstore/\w{32}", TmpFileHandler),
    (r"/api/v1/store/\w{32}", StaticFileHandler)
]

app = tornado.web.Application(endpoints)


if __name__ == "__main__":
    app.listen(listen_port)
    loop = tornado.ioloop.IOLoop.current()
    loop.add_callback(consume)
    try:
        loop.start()
    except KeyboardInterrupt:
        loop.stop()
