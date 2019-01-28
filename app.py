import asyncio
import collections
import json
import logging
import os
import re
import base64
import sys
import requests

from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from tempfile import NamedTemporaryFile
from time import time

import tornado.ioloop
import tornado.web
import tornado.httpclient
from tornado.ioloop import IOLoop
from kafkahelpers import ReconnectingClient
from requests import ConnectionError

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.errors import KafkaError
from utils import mnm
from logstash_formatter import LogstashFormatterV1
from prometheus_async.aio import time as prom_time

# Logging
LOGLEVEL = os.getenv("LOGLEVEL", "INFO")
if any("KUBERNETES" in k for k in os.environ):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(LogstashFormatterV1())
    logging.root.setLevel(LOGLEVEL)
    logging.root.addHandler(handler)
else:
    logging.basicConfig(
        level=LOGLEVEL,
        format="%(threadName)s %(levelname)s %(name)s - %(message)s"
    )

logger = logging.getLogger('upload-service')

# Valid topics config
VALID_TOPICS = []
TOPIC_CONFIG = os.getenv('TOPIC_CONFIG', '/etc/upload-service/topics.json')
with open(TOPIC_CONFIG, 'r') as f:
    data = f.read().replace("'", '"')
    topic_config = json.loads(data)

for topic in topic_config:
    for name in topic['TOPIC_NAME'].split('.'):
        VALID_TOPICS.append(name)

# Set Storage driver to use
storage_driver = os.getenv("STORAGE_DRIVER", "s3")
storage = import_module("utils.storage.{}".format(storage_driver))

# Upload content type must match this regex. Third field matches end service
content_regex = r'^application/vnd\.redhat\.([a-z0-9-]+)\.([a-z0-9-]+)\+(tgz|zip)$'

# Items in this map are _special cases_ where the service cannot be extracted
# from the Content-Type
SERVICE_MAP = {
    'application/x-gzip; charset=binary': 'advisor'
}

# set max length to 10.5 MB (one MB larger than peak)
MAX_LENGTH = int(os.getenv('MAX_LENGTH', 11010048))
LISTEN_PORT = int(os.getenv('LISTEN_PORT', 8888))
RETRY_INTERVAL = int(os.getenv('RETRY_INTERVAL', 5))  # seconds

# Maximum workers for threaded execution
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 50))

# Maximum time to wait for an archive to upload to storage
STORAGE_UPLOAD_TIMEOUT = int(os.getenv('STORAGE_UPLOAD_TIMEOUT', 60))

# dummy values for testing without a real identity
DUMMY_VALUES = {
    'principal': 'default_principal',
    'account': '000001',
    'payload_id': '1234567890abcdef',
    'url': 'http://defaulttesturl',
    'validation': 0,
    'size': 0
}

VALIDATION_QUEUE = os.getenv('VALIDATION_QUEUE', 'platform.upload.validation')

INVENTORY_URL = os.getenv('INVENTORY_URL', 'http://inventory:8080/api/hosts')

# Message Queue
MQ = os.getenv('KAFKAMQ', 'kafka:29092').split(',')
MQ_GROUP_ID = os.getenv('MQ_GROUP_ID', 'upload')


kafka_consumer = AIOKafkaConsumer(VALIDATION_QUEUE, loop=IOLoop.current().asyncio_loop,
                                  bootstrap_servers=MQ, group_id=MQ_GROUP_ID)
kafka_producer = AIOKafkaProducer(loop=IOLoop.current().asyncio_loop, bootstrap_servers=MQ,
                                  request_timeout_ms=10000, connections_max_idle_ms=None)
CONSUMER = ReconnectingClient(kafka_consumer, "consumer")
PRODUCER = ReconnectingClient(kafka_producer, "producer")

# local queue for pushing items into kafka, this queue fills up if kafka goes down
produce_queue = collections.deque([], 999)

# Executor used to run non-async/blocking tasks
thread_pool_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


with open('VERSION', 'r') as f:
    VERSION = f.readlines()[0]


def split_content(content):
    """Split the content_type to find the service name

    Arguments:
        content {str} -- content-type of the payload

    Returns:
        str -- Service name to be notified of upload
    """
    service = content.split('.')[2]
    return service


def get_service(content_type):
    """
    Returns the service that content_type maps to.
    """
    if content_type in SERVICE_MAP:
        return SERVICE_MAP[content_type]
    else:
        m = re.search(content_regex, content_type)
        if m:
            return split_content(content_type)
    raise Exception("Could not resolve a service from the given content_type")


async def handle_validation(client):
    data = await client.getmany(timeout_ms=1000, max_records=30)
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
            topic, msg, payload_id = item['topic'], item['msg'], item['msg'].get('payload_id')
            logger.info(
                "Popped data from produce queue (qsize now: %d) for topic [%s], payload_id [%s]: %s",
                len(queue), topic, payload_id, msg
            )
            try:
                await client.send_and_wait(topic, json.dumps(msg).encode("utf-8"))
                logger.info("send data for topic [%s] with payload_id [%s] succeeded", topic, payload_id)
            except KafkaError:
                queue.append(item)
                logger.error(
                    "send data for topic [%s] with payload_id [%s] failed, put back on queue (qsize now: %d)",
                    topic, payload_id, len(queue)
                )
                raise
    return send_to_preprocessors


@prom_time(mnm.uploads_handle_file_seconds)
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
            continue

        # get the payload_id. Getting the hash is temporary until consumers update
        payload_id = data['payload_id'] if 'payload_id' in data else data.get('hash')
        result = data['validation']

        logger.info('processing message: payload [%s] - %s', payload_id, result)

        if storage.ls(storage.QUARANTINE, payload_id)['ResponseMetadata']['HTTPStatusCode'] == 200:
            if result.lower() == 'success':
                mnm.uploads_validated.inc()

                url = await IOLoop.current().run_in_executor(
                    None, storage.copy, storage.QUARANTINE, storage.PERM, payload_id
                )
                data = {
                    'topic': 'platform.upload.available',
                    'msg': {
                        'id': data.get('id'),
                        'url': url,
                        'service': data.get('service'),
                        'payload_id': payload_id,
                        'account': data.get('account'),
                        'principal': data.get('principal'),
                        'b64_identity': data.get('b64_identity'),
                        'rh_account': data.get('account'),  # deprecated key, temp for backward compatibility
                        'rh_principal': data.get('principal'),  # deprecated key, temp for backward compatibility
                    }
                }
                produce_queue.append(data)
                logger.info(
                    "data for topic [%s], payload_id [%s] put on produce queue (qsize now: %d)",
                    data['topic'], payload_id, len(produce_queue)
                )
                logger.debug("payload_id [%s] data: %s", payload_id, data)
            elif result.lower() == 'failure':
                mnm.uploads_invalidated.inc()
                logger.info('payload_id [%s] rejected', payload_id)
                url = await IOLoop.current().run_in_executor(
                    None, storage.copy, storage.QUARANTINE, storage.REJECT, payload_id
                )
            else:
                logger.info('Unrecognized result: %s', result.lower())
        else:
            logger.info('payload_id [%s] no longer in quarantine', payload_id)


def post_to_inventory(identity, payload_id, values):
    headers = {'x-rh-identity': identity, 'Content-Type': 'application/json'}
    post = values['metadata']
    post['account'] = values['account']
    try:
        response = requests.post(INVENTORY_URL, json=post, headers=headers)
        if response.status_code != 200 and response.status_code != 201:
            logger.error('Failed to post to inventory: ' + response.text)
        else:
            logger.info('Payload posted to inventory: %s', payload_id)
        return response.status_code
    except ConnectionError:
        logger.error("Unable to contact inventory")


class NoAccessLog(tornado.web.RequestHandler):
    """
    A class to override tornado's logging mechanism.
    Reduce noise in the logs via GET requests we don't care about.
    """

    def _log(self):
        if LOGLEVEL == "DEBUG":
            super()._log()
        else:
            pass


class RootHandler(NoAccessLog):
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
        content_length = int(self.request.headers["Content-Length"])
        if content_length >= MAX_LENGTH:
            mnm.uploads_too_large.inc()
            return self.error(413, f"Payload too large: {content_length}. Should not exceed {MAX_LENGTH} bytes")
        try:
            get_service(self.payload_data['content_type'])
        except Exception:
            mnm.uploads_unsupported_filetype.inc()
            logger.error("Unsupported Media Type: [%s] - Request-ID [%s]", self.payload_data['content_type'], self.payload_id)
            return self.error(415, 'Unsupported Media Type')
        if re.search(content_regex, self.payload_data['content_type']).group(1) not in VALID_TOPICS:
            logger.error("Unsupported MIME type: [%s] - Request-ID [%s]", self.payload_data['content_type'], self.payload_id)
            return self.error(415, 'Unsupported MIME type')

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

        Write to storage, send message to MQ
        """
        values = {}
        # use dummy values for now if no account given
        if self.identity:
            values['account'] = self.identity['account_number']
            values['rh_account'] = self.identity['account_number']
            values['principal'] = self.identity['internal'].get('org_id') if self.identity.get('internal') else None
        else:
            values['account'] = DUMMY_VALUES['account']
            values['principal'] = DUMMY_VALUES['principal']
        values['payload_id'] = self.payload_id
        values['hash'] = self.payload_id  # provided for backward compatibility
        values['size'] = self.size
        values['service'] = self.service
        values['b64_identity'] = self.b64_identity
        if self.metadata:
            values['metadata'] = json.loads(self.metadata)
            post_to_inventory(self.b64_identity, self.payload_id, values)

        url = await self.upload(self.filename, self.tracking_id, self.payload_id)

        if url:
            values['url'] = url

            topic = 'platform.upload.' + self.service
            produce_queue.append({'topic': topic, 'msg': values})
            logger.info(
                "Data for payload_id [%s] to topic [%s] put on produce queue (qsize now: %d)",
                self.payload_id, topic, len(produce_queue)
            )

    @mnm.uploads_write_tarfile.time()
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

    def error(self, code, message):
        logger.error(message)
        self.set_status(code, message)
        self.set_header("Content-Type", "text/plain")
        self.write(message)
        return (code, message)

    @prom_time(mnm.uploads_post_time)
    async def post(self):
        """Handle POST requests to the upload endpoint

        Validate upload, get service name, create UUID, save to local storage,
        then offload for async processing
        """
        mnm.uploads_total.inc()
        self.identity = None

        if not self.request.files.get('upload') and not self.request.files.get('file'):
            return self.error(415, "Upload field not found")

        self.payload_id = self.request.headers.get('x-rh-insights-request-id')

        # TODO: pull this out once no one is using the upload field anymore
        self.payload_data = self.request.files.get('upload')[0] if self.request.files.get('upload') else self.request.files.get('file')[0]

        if self.payload_id is None:
            return self.error(400, "No payload_id assigned.  Upload failed.")

        if self.upload_validation():
            mnm.uploads_invalid.inc()
        else:
            mnm.uploads_valid.inc()
            self.tracking_id = str(self.request.headers.get('Tracking-ID', "null"))
            self.metadata = self.request.body_arguments['metadata'][0].decode('utf-8') if self.request.body_arguments.get('metadata') else None
            self.service = get_service(self.payload_data['content_type'])
            if self.request.headers.get('x-rh-identity'):
                header = json.loads(base64.b64decode(self.request.headers['x-rh-identity']))
                self.identity = header['identity']
                self.b64_identity = self.request.headers['x-rh-identity']
            self.size = int(self.request.headers['Content-Length'])
            body = self.payload_data['body']

            self.filename = await IOLoop.current().run_in_executor(None, self.write_data, body)

            self.set_status(202, "Accepted")

            # Offload the handling of the upload and producing to kafka
            asyncio.ensure_future(
                self.process_upload()
            )

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


class MetricsHandler(NoAccessLog):
    """Handle requests to the metrics
    """

    def get(self):
        self.write(mnm.generate_latest())


endpoints = [
    (r"/r/insights/platform/upload", RootHandler),
    (r"/r/insights/platform/upload/api/v1/version", VersionHandler),
    (r"/r/insights/platform/upload/api/v1/upload", UploadHandler),
    (r"/metrics", MetricsHandler)
]

app = tornado.web.Application(endpoints, max_body_size=MAX_LENGTH)


def main():
    app.listen(LISTEN_PORT)
    logger.info(f"Web server listening on port {LISTEN_PORT}")
    loop = IOLoop.current()
    loop.set_default_executor(thread_pool_executor)
    loop.spawn_callback(CONSUMER.get_callback(handle_validation))
    loop.spawn_callback(PRODUCER.get_callback(make_preprocessor(produce_queue)))
    try:
        loop.start()
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    main()
