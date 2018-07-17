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
from kiel import clients, exc
from time import sleep

from utils import storage

# Logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger('upload-service')

# Upload content type must match this regex. Third field matches end service
content_regex = r'^application/vnd\.redhat\.([a-z]+)\.([a-z]+)\+(tgz|zip)$'

# set max length to 10.5 MB (one MB larger than peak)
MAX_LENGTH = int(os.getenv('MAX_LENGTH', 11010048))
LISTEN_PORT = int(os.getenv('LISTEN_PORT', 8888))

# Maximum workers for threaded execution
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 10))

# these are dummy values since we can't yet get a principle or rh_account
values = {'principle': 'dumdum',
          'rh_account': '123456'}

# S3 buckets
QUARANTINE = os.getenv('S3_QUARANTINE', 'insights-upload-quarantine')
PERM = os.getenv('S3_PERM', 'insights-upload-perm-test')
REJECT = os.getenv('S3_REJECT', 'insights-upload-rejected')

# Message Queue
MQ = os.getenv('KAFKAMQ', 'kafka:29092').split(',')

# message queues
mqp = clients.Producer(MQ)
mqc = clients.SingleConsumer(MQ)

with open('VERSION', 'r') as f:
    VERSION = f.read()


def split_content(content):
    """Split the content-type to find the service name

    Arguments:
        content {str} -- content-type of the payload

    Returns:
        str -- Service name to be notified of upload
    """
    service = content.split('.')[2]
    return service


@tornado.gen.coroutine
def consume():
    """Connect to the message queue and consume messages from
       the 'uploadvalidation' queue
    """
    yield mqc.connect()

    try:
        while True:
            msgs = yield mqc.consume('uploadvalidation')
            if msgs:
                logger.info('recieved message')
                handle_file(msgs)
    except exc.NoBrokersError:
        logger.error('Consume Failure: No Brokers Available')


@tornado.gen.coroutine
def handle_file(msgs):
    """Determine which bucket to put a payload in based on the message
       returned from the validating service.

    Arguments:
        msgs {dict} -- The message returned by the validating service
    """
    for msg in msgs:
        hash_ = msg['hash']
        result = msg['validation']
        logger.info('processing message: %s - %s' % (hash_, result))
        if result.lower() == 'success':
            if storage.object_info(hash_, QUARANTINE):
                url = storage.transfer(hash_, QUARANTINE, PERM)
                logger.info(url)
                produce('available', {'url': url})
            else:
                logger.info('Object does not exist')
        if result.lower() == 'failure':
            if storage.object_info(hash_, QUARANTINE):
                logger.info(hash_ + ' rejected')
                url = storage.transfer(hash_, QUARANTINE, REJECT)
            else:
                logger.info('Object does not exist')


@tornado.gen.coroutine
def produce(topic, msg):
    """Produce a message to a given topic on the MQ

    Arguments:
        topic {str} -- The service name to notify
        msg {dict} -- JSON containing a rh_account, principal, payload hash,
                        and url for download
    """
    yield mqp.connect()
    try:
        yield mqp.produce(topic, json.dumps(msg))
    except exc.NoBrokersError:
        logger.error('Produce Failed: No Brokers Available')


class RootHandler(tornado.web.RequestHandler):
    """Handles requests to root
    """

    def get(self):
        """Handle GET requests to the root url
        """
        self.write("boop")

    def options(self):
        """Return a header containing the available methods
        """
        self.add_header('Allow', 'GET, HEAD, OPTIONS')


class UploadHandler(tornado.web.RequestHandler):
    """Handles requests to the upload endpoint
    """

    # * Setup the thread pool for backgrounding the upload process
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    def upload_validation(self):
        """Validate the upload using general criteria

        Returns:
            tuple -- status code and a user friendly message
        """
        if int(self.request.headers['Content-Length']) >= MAX_LENGTH:
            error = (413, 'Payload too large: ' + self.request.headers['Content-Length'] + '. Should not exceed ' + MAX_LENGTH + ' bytes')
            return error
        if re.search(content_regex, self.request.files['upload'][0]['content_type']) is None:
            error = (415, 'Unsupported Media Type')
            return error

    def get(self):
        """Handles GET requests to the upload endpoint
        """
        self.write("Accepted Content-Types: gzipped tarfile, zip file")

    @run_on_executor
    def write_data(self):
        """Writes the uploaded date to a tmp file in prepartion for upload to
           S3

           Returns:
                dict -- status tuple containing error code and message
                str -- tmp filename so it can be uploaded
        """
        with NamedTemporaryFile(delete=False) as tmp:
            tmp.write(self.request.files['upload'][0]['body'])
            tmp.flush()
            filename = tmp.name
        response = {'status': (202, 'Accepted')}
        return response, filename

    @run_on_executor
    def upload(self, filename):
        """Upload the payload to S3 Quarantine Bucket

        Arguments:
            filename {str} -- The filename to upload. Should be the tmpfile
                              created by `write_data`

        Returns:
            str -- done. used to notify upload service to send to MQ
        """
        url = storage.upload_to_s3(filename, QUARANTINE, self.hash_value)
        os.remove(filename)
        return url

    @tornado.gen.coroutine
    def post(self):
        """Handle POST requests to the upload endpoint

        Validate upload, get service name, create UUID, upload to S3, and send
        message to MQ
        """
        if not self.request.files.get('upload'):
            logger.info('Upload field not found')
            self.set_status(415, "Upload field not found")
            self.finish()
        invalid = self.upload_validation()
        if invalid:
            self.set_status(invalid[0], invalid[1])
        else:
            service = split_content(self.request.files['upload'][0]['content_type'])
            self.hash_value = uuid.uuid4().hex
            result = yield self.write_data()
            values['hash'] = self.hash_value
            self.set_status(result[0]['status'][0], result[0]['status'][1])
            self.finish()
            url = yield self.upload(result[1])
            logger.info(url)
            values['url'] = url
            while not storage.object_info(self.hash_value, QUARANTINE):
                pass
            else:
                logger.info('upload id: ' + self.hash_value)
                yield produce(service, values)

    def options(self):
        """Handle OPTIONS request to upload endpoint
        """
        self.add_header('Allow', 'GET, POST, HEAD, OPTIONS')


class VersionHandler(tornado.web.RequestHandler):
    """Handler for the `version` endpoint
    """

    def get(self):
        """Handle GET request to the `version` endpoint
        """
        response = {'version': VERSION}
        self.write(response)


endpoints = [
    (r"/", RootHandler),
    (r"/api/v1/version", VersionHandler),
    (r"/api/v1/upload", UploadHandler),
]

app = tornado.web.Application(endpoints)


if __name__ == "__main__":
    sleep(10)
    app.listen(LISTEN_PORT)
    loop = tornado.ioloop.IOLoop.current()
    loop.add_callback(consume)
    try:
        loop.start()
    except KeyboardInterrupt:
        loop.stop()
