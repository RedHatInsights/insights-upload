import asyncio
import collections
import json
import logging
import os
import re
import base64
import sys

from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from tempfile import NamedTemporaryFile
from time import sleep, time

import tornado.ioloop
import tornado.web
from tornado.ioloop import IOLoop

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.errors import KafkaError
from utils import mnm
from logstash_formatter import LogstashFormatterV1

# Logging
if any("KUBERNETES" in k for k in os.environ):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(LogstashFormatterV1())
    logging.root.setLevel(os.getenv("LOGLEVEL", "INFO"))
    logging.root.addHandler(handler)
else:
    logging.basicConfig(
        level=os.getenv("LOGLEVEL", "INFO"),
        format="%(threadName)s %(levelname)s %(name)s - %(message)s"
    )

logger = logging.getLogger('upload-service')

# Set Storage driver to use
storage_driver = os.getenv("STORAGE_DRIVER", "s3")
storage = import_module("utils.storage.{}".format(storage_driver))

# Upload content type must match this regex. Third field matches end service
content_regex = r'^application/vnd\.redhat\.([a-z0-9-]+)\.([a-z0-9-]+)\+(tgz|zip)$'

# set max length to 10.5 MB (one MB larger than peak)
MAX_LENGTH = int(os.getenv('MAX_LENGTH', 11010048))
LISTEN_PORT = int(os.getenv('LISTEN_PORT', 8888))
RETRY_INTERVAL = int(os.getenv('RETRY_INTERVAL', 5))  # seconds

# Maximum workers for threaded execution
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 50))

# Maximum time to wait for an archive to upload to storage
STORAGE_UPLOAD_TIMEOUT = int(os.getenv('STORAGE_UPLOAD_TIMEOUT', 60))

# these are dummy values since we can't yet get a principal or rh_account
DUMMY_VALUES = {
    'principal': 'default_principal',
    'rh_account': '000001',
    'payload_id': '1234567890abcdef',
    'url': 'http://defaulttesturl',
    'validation': 0,
    'size': 0
}

VALIDATION_QUEUE = os.getenv('VALIDATION_QUEUE', 'platform.upload.validation')

# Message Queue
MQ = os.getenv('KAFKAMQ', 'kafka:29092').split(',')
MQ_GROUP_ID = os.getenv('MQ_GROUP_ID', 'upload')


class MQClient(object):

    def __init__(self, client, name):
        self.client = client
        self.name = name
        self.connected = False

    def __str__(self):
        return f"MQClient {self.name} {self.client} {'connected' if self.connected else 'disconnected'}"

    async def start(self):
        while not self.connected:
            try:
                logger.info("Attempting to connect %s client.", self.name)
                await self.client.start()
                logger.info("%s client connected successfully.", self.name)
                self.connected = True
            except KafkaError:
                logger.exception("Failed to connect %s client, retrying in %d seconds.", self.name, RETRY_INTERVAL)
                await asyncio.sleep(RETRY_INTERVAL)

    async def work(self, worker):
        try:
            await worker(self.client)
        except KafkaError:
            logger.exception("Encountered exception while working with %s client, reconnecting.", self.name)
            self.connected = False

    def run(self, worker):
        async def _f():
            while True:
                await self.start()
                await self.work(worker)
        return _f


mqc = AIOKafkaConsumer(
    VALIDATION_QUEUE, loop=IOLoop.current().asyncio_loop, bootstrap_servers=MQ,
    group_id=MQ_GROUP_ID
)
mqp = AIOKafkaProducer(
    loop=IOLoop.current().asyncio_loop, bootstrap_servers=MQ, request_timeout_ms=10000,
    connections_max_idle_ms=None
)
CONSUMER = MQClient(mqc, "consumer")
PRODUCER = MQClient(mqp, "producer")

# local queue for pushing items into kafka, this queue fills up if kafka goes down
produce_queue = collections.deque([], 999)

# Executor used to run non-async/blocking tasks
thread_pool_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


with open('VERSION', 'r') as f:
    VERSION = f.read()


def split_content(content):
    """Split the content_type to find the service name

    Arguments:
        content {str} -- content-type of the payload

    Returns:
        str -- Service name to be notified of upload
    """
    service = content.split('.')[2]
    return service


async def handle_validation(client):
    data = await client.getmany()
    for tp, msgs in data.items():
        if tp.topic == VALIDATION_QUEUE:
            await handle_file(msgs)


def make_preprocessor(queue=None):
    queue = produce_queue if queue is None else queue

    async def send_to_preprocessors(client):
        if not queue:
            await asyncio.sleep(0.1)
        else:
            item = queue.popleft()
            topic, msg = item["topic"], item["msg"]
            logger.info(
                "Popped item from produce queue (qsize: %d): topic %s: %s",
                len(queue), topic, msg
            )
            try:
                await client.send_and_wait(topic, json.dumps(msg).encode("utf-8"))
            except KafkaError:
                queue.append(item)
                raise
    return send_to_preprocessors


async def handle_file(msgs):
    """Determine which bucket to put a payload in based on the message
       returned from the validating service.

    storage.copy operations are not async so we offload to the executor

    Arguments:
        msgs -- list of kafka messages consumed on validation topic
    """
    for msg in msgs:
        try:
            data = json.loads(msg.value)
        except ValueError:
            logger.error("handle_file(): unable to decode msg as json: {}".format(msg.value))
            continue

        if 'payload_id' not in data and 'hash' not in data:
            logger.error("payload_id or hash not in message. Payload not removed from quarantine.")
            return

        # get the payload_id. Getting the hash is temporary until consumers update
        payload_id = data['payload_id'] if 'payload_id' in data else data.get('hash')
        result = data['validation']

        logger.info('processing message: %s - %s', payload_id, result)
        if result.lower() == 'success':
            url = await IOLoop.current().run_in_executor(
                None, storage.copy, storage.QUARANTINE, storage.PERM, payload_id
            )
            logger.info(url)
            produce_queue.append(
                {
                    'topic': 'platform.upload.available',
                    'msg': {'url': url,
                            'payload_id': payload_id}
                }
            )
        elif result.lower() == 'failure':
            logger.info('%s rejected', payload_id)
            url = await IOLoop.current().run_in_executor(
                None, storage.copy, storage.QUARANTINE, storage.REJECT, payload_id
            )
        else:
            logger.info('Unrecognized result: %s', result.lower())


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

    async def upload(self, filename, tracking_id, payload_id):
        """Write the payload to the configured storage

        Storage write and os file operations are not async so we offload to executor.

        Arguments:
            filename {str} -- The filename to upload. Should be the tmpfile
                              created by `write_data`
            tracking_id {str} -- The tracking ID sent by the client
            payload_id {str} -- the unique ID for this upload generated by 3Scale at time of POST

        Returns:
            str -- URL of uploaded file if successful
            None if upload failed
        """

        upload_start = time()
        logger.info("tracking id [%s] payload_id [%s] attempting upload", tracking_id, payload_id)

        try:
            url = await IOLoop.current().run_in_executor(
                None, storage.write, filename, storage.QUARANTINE, payload_id
            )
            elapsed = time() - upload_start

            logger.info(
                "tracking id [%s] payload_id [%s] uploaded! elapsed [%fsec] url [%s]",
                tracking_id, payload_id, elapsed, url
            )

            return url
        except Exception:
            elapsed = time() - upload_start
            logger.exception(
                "Exception hit uploading: tracking id [%s] payload_id [%s] elapsed [%fsec]",
                tracking_id, payload_id, elapsed
            )
        finally:
            await IOLoop.current().run_in_executor(None, os.remove, filename)

    async def process_upload(self):
        """Process the uploaded file we have received.

        Arguments:
            filename {str} -- The filename to upload. Should be the tmpfile
                              created by `write_data`
            size {int} -- content-length of the uploaded filename
            tracking_id {str} -- The tracking ID sent by the client
            payload_id {str} -- the unique ID for this upload generated by 3Scale at time of POST
            identity {str} -- identity pulled from request headers (if present)
            service {str} -- The service this upload is intended for

        Write to storage, send message to MQ, send metrics to influxDB
        """
        values = {}
        # use dummy values for now if no account given
        logger.info('identity - %s', self.identity)
        if self.identity:
            values['rh_account'] = self.identity['account_number']
            values['principal'] = self.identity['org_id']
        else:
            values['rh_account'] = DUMMY_VALUES['rh_account']
            values['principal'] = DUMMY_VALUES['principal']
        values['payload_id'] = self.payload_id
        values['hash'] = self.payload_id  # provided for backward compatibility
        values['size'] = self.size
        values['service'] = self.service
        if self.metadata:
            values['metadata'] = json.loads(self.metadata)

        url = await self.upload(self.filename, self.tracking_id, self.payload_id)

        if url:
            values['url'] = url

            produce_queue.append({'topic': 'platform.upload.' + self.service, 'msg': values})
            logger.info(
                "Data for payload_id [%s] put on produce queue (qsize: %d)",
                self.payload_id, len(produce_queue)
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
        self.identity = None

        if not self.request.files.get('upload'):
            logger.info('Upload field not found')
            self.set_status(415, "Upload field not found")
            return

        self.payload_id = self.request.headers.get('x-rh-insights-request-id')

        if self.payload_id is None:
            msg = "No payload_id assigned. Upload Failed"
            logger.error(msg)
            self.set_header("Content-Type", "text/plain")
            self.set_status(400)
            self.write(msg)
            return

        invalid = self.upload_validation()

        if invalid:
            self.set_status(invalid[0], invalid[1])
            return
        else:
            self.tracking_id = str(self.request.headers.get('Tracking-ID', "null"))
            self.metadata = self.request.body_arguments['metadata'][0].decode('utf-8') if self.request.body_arguments.get('metadata') else None
            self.service = split_content(self.request.files['upload'][0]['content_type'])
            if self.request.headers.get('x-rh-identity'):
                logger.info('x-rh-identity: %s', base64.b64decode(self.request.headers['x-rh-identity']))
                header = json.loads(base64.b64decode(self.request.headers['x-rh-identity']))
                self.identity = header['identity']
            self.size = int(self.request.headers['Content-Length'])
            body = self.request.files['upload'][0]['body']

            self.filename = await IOLoop.current().run_in_executor(None, self.write_data, body)

            response = {'status': (202, 'Accepted')}
            self.set_status(response['status'][0], response['status'][1])

            # Offload the handling of the upload and producing to kafka
            asyncio.ensure_future(
                self.process_upload()
            )
            return

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
    (r"/r/insights/platform/upload", RootHandler),
    (r"/r/insights/platform/upload/api/v1/version", VersionHandler),
    (r"/r/insights/platform/upload/api/v1/upload", UploadHandler),
]

app = tornado.web.Application(endpoints, max_body_size=MAX_LENGTH)


def main():
    sleep(10)
    app.listen(LISTEN_PORT)
    logger.info(f"Web server listening on port {LISTEN_PORT}")
    loop = IOLoop.current()
    loop.set_default_executor(thread_pool_executor)
    loop.spawn_callback(CONSUMER.run(handle_validation))
    loop.spawn_callback(PRODUCER.run(make_preprocessor(produce_queue)))
    try:
        loop.start()
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    main()
