import tornado.ioloop
import tornado.web
import os
import re
import uuid
import json
import logging

from tempfile import NamedTemporaryFile
from tornado.ioloop import IOLoop
from tornado.queues import Queue, QueueFull
from kiel import clients, exc
from time import time, sleep

from utils.storage import s3 as storage
from utils import mnm

# Logging
logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"))
logger = logging.getLogger('upload-service')

# Upload content type must match this regex. Third field matches end service
content_regex = r'^application/vnd\.redhat\.([a-z]+)\.([a-z]+)\+(tgz|zip)$'

# set max length to 10.5 MB (one MB larger than peak)
MAX_LENGTH = int(os.getenv('MAX_LENGTH', 11010048))
LISTEN_PORT = int(os.getenv('LISTEN_PORT', 8888))

# Maximum workers for threaded execution
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 10))

# these are dummy values since we can't yet get a principal or rh_account
values = {'principal': 'default_principal',
          'rh_account': '000001',
          'hash': 'abcdef123456',
          'url': 'http://defaulttesturl',
          'validation': 0,
          'size': 0}

# Message Queue
MQ = os.getenv('KAFKAMQ', 'kafka:29092').split(',')

# message queues
mqp = clients.Producer(MQ)
mqc = clients.SingleConsumer(MQ)

# local queue for pushing items into kafka, this queue fills up if kafka goes down
produce_queue = Queue(maxsize=999)

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


async def consumer():
    """Consume indefinitely from the 'uploadvalidation' queue.
    """
    connected = False
    while True:
        # If not connected, attempt to connect...
        if not connected:
            try:
                logger.info("Consume client not connected, attempting to connect...")
                await mqc.connect()
                logger.info("Consumer client connected!")
                connected = True
            except exc.NoBrokersError:
                logger.error('Consume client connect failed: No Brokers Available')
                await tornado.gen.Task(IOLoop.current().add_timeout, time() + 5)
                continue

        # Consume
        try:
            msgs = await mqc.consume('uploadvalidation')
            if msgs:
                logger.info('recieved message')
                handle_file(msgs)
        except exc.NoBrokersError:
            logger.error('Consume Failed: No Brokers Available')
            connected = False


async def producer():
    """Produce items sitting in our local produce_queue to kafka

    An item is a dict with keys 'topic' and 'msg', which contain:
        topic {str} -- The service name to notify
        msg {dict} -- JSON containing a rh_account, principal, payload hash,
                        and url for download
    """
    connected = False
    while True:
        # If not connected to kafka, attempt to connect...
        if not connected:
            try:
                logger.info("Producer client not connected, attempting to connect...")
                await mqp.connect()
                logger.info("Producer client connected!")
                connected = True
            except exc.NoBrokersError:
                logger.error('Producer client connect failed: No Brokers Available')
                await tornado.gen.Task(IOLoop.current().add_timeout, time() + 5)
                continue

        # Pull item off our queue to produce
        item = await produce_queue.get()
        topic = item['topic']
        msg = item['msg']
        try:
            await mqp.produce(topic, json.dumps(msg))
            logger.info("Produced on topic %s: %s", topic, msg)
        except exc.NoBrokersError:
            logger.error('Produce Failed: No Brokers Available')
            connected = False
            # Put the item back on the queue so we can push it when we reconnect
            try:
                produce_queue.put_nowait(item)
            except QueueFull:
                logger.error('Producer queue is full, item dropped')


async def handle_file(msgs):
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
            url = storage.copy(storage.QUARANTINE, storage.PERM, hash_)
            logger.info(url)
            await produce_queue.put(
                {
                    'topic': 'available',
                    'msg': {'url': url}
                }
            )
        elif result.lower() == 'failure':
            logger.info(hash_ + ' rejected')
            url = storage.copy(storage.QUARANTINE, storage.REJECT, hash_)
        else:
            logger.info('Unrecognized result: ' + result.lower())


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

    def upload_validation(self):
        """Validate the upload using general criteria

        Returns:
            tuple -- status code and a user friendly message
        """
        if int(self.request.headers['Content-Length']) >= MAX_LENGTH:
            error = (413, 'Payload too large: ' + self.request.headers['Content-Length'] + '. Should not exceed ' + str(MAX_LENGTH) + ' bytes')
            return error
        if re.search(content_regex, self.request.files['upload'][0]['content_type']) is None:
            error = (415, 'Unsupported Media Type')
            return error

    def get(self):
        """Handles GET requests to the upload endpoint
        """
        self.write("Accepted Content-Types: gzipped tarfile, zip file")

    async def write_data(self):
        """Writes the uploaded date to a tmp file in prepartion for writing to
           storage

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

    async def upload(self, filename):
        """Write the payload to the configured storage

        Arguments:
            filename {str} -- The filename to upload. Should be the tmpfile
                              created by `write_data`

        Returns:
            str -- done. used to notify upload service to send to MQ
        """
        url = storage.write(filename, storage.QUARANTINE, self.hash_value)
        os.remove(filename)
        return url

    async def post(self):
        """Handle POST requests to the upload endpoint

        Validate upload, get service name, create UUID, write to storage, and send
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
            response, filename = await self.write_data()
            values['validation'] = 1
            values['hash'] = self.hash_value
            values['size'] = int(self.request.headers['Content-Length'])
            values['service'] = service
            self.set_status(response['status'][0], response['status'][1])
            self.add_header('uuid', self.hash_value)
            self.finish()
            url = await self.upload(filename)
            logger.info(url)
            values['url'] = url
            mnm.send_to_influxdb(values)
            while not storage.ls(storage.QUARANTINE, self.hash_value):
                pass
            else:
                logger.info('upload id: ' + self.hash_value)
                await produce_queue.put({'topic': service, 'msg': values})

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


class StatusHandler(tornado.web.RequestHandler):

    async def get(self):

        response = {"upload-service": "up",
                    "message queue": "down",
                    "Long Term Storage": "down",
                    "Quarantine Storage": "down",
                    "Rejected Storage": "down"}

        if storage.up_check(storage.PERM):
            response['Long Term Storage'] = "up"
        if storage.up_check(storage.QUARANTINE):
            response['Quarantine Storage'] = "up"
        if storage.up_check(storage.REJECT):
            response['Rejected Storage'] = "up"
        try:
            await mqc.connect()
            response['message queue'] = "up"
        except exc.NoBrokersError:
            response['message queue'] = "down"

        self.write(response)


endpoints = [
    (r"/", RootHandler),
    (r"/api/v1/version", VersionHandler),
    (r"/api/v1/upload", UploadHandler),
    (r"/api/v1/status", StatusHandler),
]

app = tornado.web.Application(endpoints, max_body_size=MAX_LENGTH)


def main():
    sleep(10)
    app.listen(LISTEN_PORT)
    loop = IOLoop.current()
    loop.add_callback(consumer)
    loop.add_callback(producer)
    try:
        loop.start()
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    main()
