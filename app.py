import asyncio
import json
import logging
import os
import re
import base64
import uuid
import signal

from importlib import import_module
from tempfile import NamedTemporaryFile
from time import time, sleep

import tornado.ioloop
import tornado.web
from tornado.ioloop import IOLoop

from utils import mnm, config, log
from ctx import get_extra
import messaging
from defer import defer
from prometheus_async.aio import time as prom_time

log.setup_logging()
logger = logging.getLogger('upload-service')

if not config.DEVMODE:
    VALID_TOPICS = config.get_valid_topics()

# Set Storage driver to use
storage = import_module("utils.storage.{}".format(config.STORAGE_DRIVER))

# Upload content type must match this regex. Third field matches end service
content_regex = r'^application/vnd\.redhat\.(?P<service>[a-z0-9-]+)\.(?P<category>[a-z0-9-]+).*'
produce_queue = messaging.produce_queue

current_archives = []


if config.DEVMODE:
    BUILD_DATE = 'devmode'
else:
    BUILD_DATE = config.get_commit_date(config.BUILD_ID)


def _filter(k, v):
    if k == "display_name":
        return 2 > len(v) > 200
    else:
        return v not in ("", [], {}, None, ())


def clean_up_metadata(facts):
    """
    Empty values need to be stripped from metadata prior to posting to inventory.
    Display_name must be greater than 1 and less than 200 characters.
    """
    return {k: v for k, v in facts.items() if _filter(k, v)}


def get_service(content_type):
    """
    Returns the service that content_type maps to.
    """
    if content_type in config.SERVICE_MAP:
        return config.SERVICE_MAP[content_type]
    else:
        m = re.search(content_regex, content_type)
        if m:
            return m.groupdict()
    raise Exception("Could not resolve a service from the given content_type")


class NoAccessLog(tornado.web.RequestHandler):
    """
    A class to override tornado's logging mechanism.
    Reduce noise in the logs via GET requests we don't care about.
    """

    def _log(self):
        if log.LOGLEVEL == "DEBUG":
            super()._log()
        else:
            pass


class RootHandler(NoAccessLog):
    """Handles requests to document root
    """

    def get(self):
        """Handle GET requests to the root url
        ---
        description: Used for OpenShift Liveliness probes
        responses:
            200:
                description: OK
                content:
                    text/plain:
                        schema:
                            type: string
                            example: boop
        """
        self.write("boop")

    def options(self):
        """Return a header containing the available methods
        ---
        description: Add a header containing allowed methods
        responses:
            200:
                description: OK
                headers:
                    Allow:
                        description: Allowed methods
                        schema:
                            type: string
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
        extra = get_extra(account=self.account, request_id=self.payload_id)
        content_length = int(self.request.headers["Content-Length"])
        if content_length >= config.MAX_LENGTH:
            mnm.uploads_too_large.inc()
            logger.error("Payload too large. Request ID [%s] - Length %s", self.payload_id, str(config.MAX_LENGTH), extra=extra)
            return self.error(413, f"Payload too large: {content_length}. Should not exceed {config.MAX_LENGTH} bytes", **extra)
        try:
            serv_dict = get_service(self.payload_data['content_type'])
        except Exception:
            mnm.uploads_unsupported_filetype.inc()
            logger.exception("Unsupported Media Type: [%s] - Request-ID [%s]", self.payload_data['content_type'], self.payload_id, extra=extra)
            return self.error(415, 'Unsupported Media Type', **extra)
        if not config.DEVMODE and serv_dict["service"] not in VALID_TOPICS:
            logger.error("Unsupported MIME type: [%s] - Request-ID [%s]", self.payload_data['content_type'], self.payload_id, extra=extra)
            return self.error(415, 'Unsupported MIME type', **extra)

    def get(self):
        """Handles GET requests to the upload endpoint
        ---
        description: Get accepted content types
        responses:
            200:
                description: OK
                content:
                    text/plain:
                        schema:
                            type: string
                            example: 'Accepted Content-Types: gzipped tarfile, zip file'
        """
        self.write("Accepted Content-Types: gzipped tarfile, zip file")

    async def upload(self, filename, payload_id, identity):
        """Write the payload to the configured storage

        Storage write and os file operations are not async so we offload to executor.

        Arguments:
            filename {str} -- The filename to upload. Should be the tmpfile
                              created by `write_data`
            payload_id {str} -- the unique ID for this upload generated by 3Scale at time of POST

        Returns:
            str -- URL of uploaded file if successful
            None if upload failed
        """
        user_agent = self.request.headers.get("User-Agent")
        extra = get_extra(account=self.account, request_id=payload_id)

        upload_start = time()
        try:
            url = await defer(storage.write, filename, storage.PERM, payload_id, self.account, user_agent)
            elapsed = time() - upload_start

            logger.info(
                "payload_id [%s] uploaded! elapsed [%fsec] url [%s]",
                payload_id, elapsed, url, extra=extra)

            return url
        except Exception:
            elapsed = time() - upload_start
            logger.exception(
                "Exception hit uploading: payload_id [%s] elapsed [%fsec]",
                payload_id, elapsed, extra=extra)
        finally:
            await defer(os.remove, filename)

    async def process_upload(self):
        """Process the uploaded file we have received.

        Arguments:
            filename {str} -- The filename to upload. Should be the tmpfile
                              created by `write_data`
            size {int} -- content-length of the uploaded filename
            payload_id {str} -- the unique ID for this upload generated by 3Scale at time of POST
            identity {str} -- identity pulled from request headers (if present)
            service {str} -- The service this upload is intended for

        Write to storage, send message to MQ
        """
        extra = get_extra(account=self.account, request_id=self.payload_id)
        values = {}
        # use dummy values for now if no account given
        if self.identity:
            values['account'] = self.account
            values['rh_account'] = self.account
            values['principal'] = self.identity['internal'].get('org_id') if self.identity.get('internal') else None
        else:
            values['account'] = config.DUMMY_VALUES['account']
            values['principal'] = config.DUMMY_VALUES['principal']
        values['payload_id'] = self.payload_id
        values['hash'] = self.payload_id  # provided for backward compatibility
        values['size'] = self.size
        values['service'] = self.service
        values['category'] = self.category
        values['b64_identity'] = self.b64_identity
        if self.metadata:
            with mnm.uploads_json_loads.labels(key="process_upload").time():
                values['metadata'] = clean_up_metadata(json.loads(self.metadata))

        url = await self.upload(self.filename, self.payload_id, self.identity)

        if url:
            values['url'] = url
            topic = 'platform.upload.' + self.service
            mnm.uploads_produced_to_topic.labels(topic=topic).inc()
            current_archives.append(self.payload_id)
            produce_queue.append({'topic': topic, 'msg': values})
            logger.info(
                "Data for payload_id [%s] to topic [%s] put on produce queue (qsize now: %d)",
                self.payload_id, topic, len(produce_queue), extra=extra
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

    def error(self, code, message, **kwargs):
        logger.error(message, extra=kwargs)
        self.set_status(code, message)
        self.set_header("Content-Type", "text/plain")
        self.write(message)
        return (code, message)

    @prom_time(mnm.uploads_post_time)
    async def post(self):
        """Handle POST requests to the upload endpoint

        Validate upload, get service name, create UUID, save to local storage,
        then offload for async processing
        ---
        description: Process Insights archive
        responses:
            202:
                description: Upload payload accepted
            413:
                description: Payload too large
            415:
                description: Upload field not found
        """
        mnm.uploads_total.inc()
        self.payload_id = self.request.headers.get('x-rh-insights-request-id', uuid.uuid4().hex)
        self.account = "unknown"

        # is this really ok to be optional?
        self.b64_identity = self.request.headers.get('x-rh-identity')
        if self.b64_identity:
            with mnm.uploads_json_loads.labels(key="post").time():
                header = json.loads(base64.b64decode(self.b64_identity))
            self.identity = header['identity']
            self.account = self.identity["account_number"]

        extra = get_extra(account=self.account, request_id=self.payload_id)

        if not self.request.files.get('upload') and not self.request.files.get('file'):
            return self.error(
                415,
                "Upload field not found",
                files=list(self.request.files),
                **extra
            )

        # TODO: pull this out once no one is using the upload field anymore
        self.payload_data = self.request.files.get('upload')[0] if self.request.files.get('upload') else self.request.files.get('file')[0]

        if self.payload_id is None:
            return self.error(400, "No payload_id assigned.  Upload failed.", **extra)

        if self.upload_validation():
            mnm.uploads_invalid.inc()
        else:
            mnm.uploads_valid.inc()
            self.metadata = self.__get_metadata_from_request()
            service_dict = get_service(self.payload_data['content_type'])
            self.service = service_dict["service"]
            self.category = service_dict["category"]
            self.size = int(self.request.headers['Content-Length'])
            body = self.payload_data['body']

            self.filename = await defer(self.write_data, body)

            self.set_status(202, "Accepted")

            # Offload the handling of the upload and producing to kafka
            asyncio.ensure_future(
                self.process_upload()
            )

    def options(self):
        """Handle OPTIONS request to upload endpoint
        ---
        description: Add a header containing allowed methods
        responses:
            200:
                description: OK
                headers:
                    Allow:
                        description: Allowed methods
                        schema:
                            type: string
        """
        self.add_header('Allow', 'GET, POST, HEAD, OPTIONS')

    def __get_metadata_from_request(self):
        if self.request.files.get('metadata'):
            return self.request.files['metadata'][0]['body'].decode('utf-8')
        elif self.request.body_arguments.get('metadata'):
            return self.request.body_arguments['metadata'][0].decode('utf-8')


class VersionHandler(tornado.web.RequestHandler):
    """Handler for the `version` endpoint
    """

    def get(self):
        """Handle GET request to the `version` endpoint
        ---
        description: Get version identifying information
        responses:
            200:
                description: OK
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                commit:
                                    type: string
                                    example: ab3a3a90b48bb1101a287b754d33ac3b2316fdf2
                                date:
                                    type: string
                                    example: '2019-03-19T14:17:27Z'
        """
        response = {'commit': config.BUILD_ID,
                    'date': BUILD_DATE}
        self.write(response)


class MetricsHandler(NoAccessLog):
    """Handle requests to the metrics
    """

    def get(self):
        """Get metrics for upload service
        ---
        description: Get metrics for upload service
        responses:
            200:
                description: OK
                content:
                    text/plain:
                        schema:
                            type: string
        """
        self.write(mnm.generate_latest())


class SpecHandler(tornado.web.RequestHandler):
    """Handle requests for service's API Spec
    """

    def get(self):
        """Get the openapi/swagger spec for the upload service
        ---
        description: Get openapi spec for upload service
        responses:
            200:
                description: OK
        """
        response = config.spec.to_dict()
        self.write(response)


def get_app():
    endpoints = [
        (config.API_PREFIX, RootHandler),
        (config.API_PREFIX + "/v1/version", VersionHandler),
        (config.API_PREFIX + "/v1/upload", UploadHandler),
        (config.API_PREFIX + "/v1/openapi.json", SpecHandler),
        (r"/r/insights/platform/upload", RootHandler),
        (r"/r/insights/platform/upload/api/v1/version", VersionHandler),
        (r"/r/insights/platform/upload/api/v1/upload", UploadHandler),
        (r"/r/insights/platform/upload/api/v1/openapi.json", SpecHandler),
        (r"/metrics", MetricsHandler)
    ]

    for urlSpec in endpoints:
        config.spec.path(urlspec=urlSpec)

    return tornado.web.Application(endpoints, max_body_size=config.MAX_LENGTH)


def signal_handler(signal, frame):
    loop = IOLoop.current()
    logger.info("Recieved Exit Signal: %s", signal)
    loop.spawn_callback(shutdown)


async def shutdown():
    loop = IOLoop.current()
    logger.debug("Stopping Server")
    messaging.stop()
    while len(current_archives) > 0:
        logger.DEBUG("Remaing archives: %s", len(current_archives))
        sleep(1)
    loop.stop()
    logger.info("Ingress Shutdown")
    logging.shutdown()


def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    app = get_app()
    app.listen(config.LISTEN_PORT)
    logger.info(f"Web server listening on port {config.LISTEN_PORT}")
    loop = IOLoop.current()
    messaging.start(loop, current_archives)
    loop.start()


if __name__ == "__main__":
    main()
