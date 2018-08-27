import asyncio
import collections
import json
import logging
import os
import re
import uuid
import base64

from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from tempfile import NamedTemporaryFile
from time import sleep, time

import tornado.ioloop
import tornado.web
from tornado.ioloop import IOLoop

from kiel import clients, exc
from utils import mnm

# Logging
logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="%(asctime)s %(threadName)s %(levelname)s -- %(message)s"
)
logger = logging.getLogger('upload-service')

# Set Storage driver to use
storage_driver = os.getenv("STORAGE_DRIVER", "s3")
storage = import_module("utils.storage.{}".format(storage_driver))

# Upload content type must match this regex. Third field matches end service
content_regex = r'^application/vnd\.redhat\.([a-z]+)\.([a-z]+)\+(tgz|zip)$'

# set max length to 10.5 MB (one MB larger than peak)
MAX_LENGTH = int(os.getenv('MAX_LENGTH', 11010048))
LISTEN_PORT = int(os.getenv('LISTEN_PORT', 8888))

# Maximum workers for threaded execution
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 50))

# Maximum time to wait for an archive to upload to storage
STORAGE_UPLOAD_TIMEOUT = int(os.getenv('STORAGE_UPLOAD_TIMEOUT', 60))

# these are dummy values since we can't yet get a principal or rh_account
DUMMY_VALUES = {
    'principal': 'default_principal',
    'rh_account': '000001',
    'hash': 'abcdef123456',
    'url': 'http://defaulttesturl',
    'validation': 0,
    'size': 0
}

# Message Queue
MQ = os.getenv('KAFKAMQ', 'kafka:29092').split(',')

# message queues
mqp = clients.Producer(MQ)
mqc = clients.SingleConsumer(MQ)

# local queue for pushing items into kafka, this queue fills up if kafka goes down
produce_queue = collections.deque([], 999)

# Executor used to run non-async/blocking tasks
thread_pool_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


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
                await asyncio.sleep(5)
                continue

        # Consume
        try:
            msgs = await mqc.consume('uploadvalidation')
            if msgs:
                logger.info('recieved message')
                await handle_file(msgs)
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
                await asyncio.sleep(5)
                continue

        # Pull items off our queue to produce
        if len(produce_queue) == 0:
            await asyncio.sleep(0.01)
            continue
        for _ in range(0, len(produce_queue)):
            item = produce_queue.popleft()
            topic = item['topic']
            msg = item['msg']
            logger.info(
                "Popped item from produce queue (qsize: %d): topic %s: %s",
                len(produce_queue), topic, msg
            )
            try:
                await mqp.produce(topic, json.dumps(msg))
                logger.info("Produced on topic %s: %s", topic, msg)
            except exc.NoBrokersError:
                logger.error('Produce Failed: No Brokers Available')
                connected = False
                # Put the item back on the queue so we can push it when we reconnect
                produce_queue.appendleft(item)


async def handle_file(msgs):
    """Determine which bucket to put a payload in based on the message
       returned from the validating service.

    storage.copy operations are not async so we offload to the executor

    Arguments:
        msgs {dict} -- The message returned by the validating service
    """
    for msg in msgs:
        hash_ = msg['hash']
        result = msg['validation']
        logger.info('processing message: %s - %s' % (hash_, result))
        if result.lower() == 'success':
            url = await IOLoop.current().run_in_executor(
                None, storage.copy, storage.QUARANTINE, storage.PERM, hash_
            )
            logger.info(url)
            produce_queue.append(
                {
                    'topic': 'available',
                    'msg': {'url': url}
                }
            )
        elif result.lower() == 'failure':
            logger.info(hash_ + ' rejected')
            url = await IOLoop.current().run_in_executor(
                None, storage.copy, storage.QUARANTINE, storage.REJECT, hash_
            )
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

    async def upload(self, filename, tracking_id, hash_value):
        """Write the payload to the configured storage

        Storage write and os file operations are not async so we offload to executor.

        Arguments:
            filename {str} -- The filename to upload. Should be the tmpfile
                              created by `write_data`
            tracking_id {str} -- The tracking ID sent by the client
            hash_value {str} -- the uuid for this upload generated by us at time of POST

        Returns:
            str -- URL of uploaded file if successful
            None if upload failed
        """
        logger.info("tracking id [%s] hash [%s] attempting upload", tracking_id, hash_value)

        success = False
        upload_start = time()
        try:
            url, callback = await IOLoop.current().run_in_executor(
                None, storage.write, filename, storage.QUARANTINE, hash_value
            )
        except Exception:
            logger.exception(
                "Exception hit uploading: tracking id [%s] hash [%s]",
                tracking_id, hash_value
            )
        else:
            for count in range(0, STORAGE_UPLOAD_TIMEOUT * 10):
                if callback.percentage >= 100:
                    success = True
                    break
                await asyncio.sleep(.01)  # to avoid baking CPU while looping

        await IOLoop.current().run_in_executor(None, os.remove, filename)

        if not success:
            # Upload failed, return None
            logger.error(
                "upload id: %s upload failed or timed out after %dsec!",
                hash_value, STORAGE_UPLOAD_TIMEOUT
            )
            return None

        elapsed = callback.time_last_updated - upload_start
        logger.info(
            "tracking id [%s] hash [%s] uploaded! elapsed [%fsec] url [%s]",
            tracking_id, hash_value, elapsed, url
        )

        return url

    async def process_upload(self, filename, size, tracking_id, hash_value, identity, service):
        """Process the uploaded file we have received.

        Arguments:
            filename {str} -- The filename to upload. Should be the tmpfile
                              created by `write_data`
            size {int} -- content-length of the uploaded filename
            tracking_id {str} -- The tracking ID sent by the client
            hash_value {str} -- the uuid for this upload generated by us at time of POST
            identity {str} -- identity pulled from request headers (if present)
            service {str} -- The service this upload is intended for

        Write to storage, send message to MQ, send metrics to influxDB
        """
        values = {}
        # use dummy values for now if no account given
        logger.info('identity - %s', identity)
        if identity:
            values['rh_account'] = identity['account_number']
            values['principal'] = identity['org_id']
        else:
            values['rh_account'] = DUMMY_VALUES['rh_account']
            values['principal'] = DUMMY_VALUES['principal']
        values['validation'] = 1
        values['hash'] = hash_value
        values['size'] = size
        values['service'] = service

        url = await self.upload(filename, tracking_id, hash_value)

        if url:
            values['url'] = url

            produce_queue.append({'topic': service, 'msg': values})
            logger.info(
                "Data for hash [%s] put on produce queue (qsize: %d)",
                hash_value, len(produce_queue)
            )

            # TODO: send a metric to influx for a failed upload too?
            IOLoop.current().run_in_executor(None, mnm.send_to_influxdb, values)

    def write_data(self, body):
        """Writes the uploaded data to a tmp file in prepartion for writing to
           storage

        OS file operations are not async so this should run in executor.

        Arguments:
            body -- upload body content

        Returns:
            str -- tmp filename so it can be uploaded
        """
        with NamedTemporaryFile(delete=False) as tmp:
            tmp.write(body)
            tmp.flush()
            filename = tmp.name
        return filename

    async def post(self):
        """Handle POST requests to the upload endpoint

        Validate upload, get service name, create UUID, save to local storage,
        then offload for async processing
        """
        identity = False
        if not self.request.files.get('upload'):
            logger.info('Upload field not found')
            self.set_status(415, "Upload field not found")
            await self.finish()
        invalid = self.upload_validation()
        if invalid:
            self.set_status(invalid[0], invalid[1])
            await self.finish()
        else:
            tracking_id = str(self.request.headers.get('Tracking-ID', "null"))
            service = split_content(self.request.files['upload'][0]['content_type'])
            if self.request.headers.get('x-rh-identity'):
                logger.info('x-rh-identity: %s', base64.b64decode(self.request.headers['x-rh-identity']))
                header = json.loads(base64.b64decode(self.request.headers['x-rh-identity']))
                identity = header['identity']
            size = int(self.request.headers['Content-Length'])
            hash_value = uuid.uuid4().hex
            body = self.request.files['upload'][0]['body']

            filename = await IOLoop.current().run_in_executor(None, self.write_data, body)

            response = {'status': (202, 'Accepted')}
            self.set_status(response['status'][0], response['status'][1])
            self.add_header('uuid', hash_value)

            # Offload the handling of the upload and producing to kafka
            asyncio.ensure_future(
                self.process_upload(filename, size, tracking_id, hash_value, identity, service)
            )
            await self.finish()

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

        response = {"upload_service": "up",
                    "message_queue": "down",
                    "long_term_storage": "down",
                    "quarantine_storage": "down",
                    "rejected_storage": "down"}

        if storage.up_check(storage.PERM):
            response['long_term_storage'] = "up"
        if storage.up_check(storage.QUARANTINE):
            response['quarantine_storage'] = "up"
        if storage.up_check(storage.REJECT):
            response['rejected_storage'] = "up"
        try:
            await mqc.connect()
            response['message_queue'] = "up"
        except exc.NoBrokersError:
            response['message_queue'] = "down"

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
    loop.set_default_executor(thread_pool_executor)
    loop.spawn_callback(consumer)
    loop.spawn_callback(producer)
    try:
        loop.start()
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    main()
