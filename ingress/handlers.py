import tornado.web
import json
import base64
import asyncio
import uuid
import logging

from prometheus_async.aio import time as prom_time

import config
import metrics
import log
from utils import filechecks

logger = logging.getLogger("ingress")

class NoAccessLog(tornado.web.RequestHandler):
    """
    A class to override tornado's logger.
    Reduce noise in the logs via GET requests we don't care about
    """

    def _log(self):
        if log.LOGLEVEL == "DEBUG":
            super()._log()
        else:
            pass


class UploadHandler(tornado.web.RequestHandler):
    """Handles requests to the upload endpoint
    """

    def initialize(self, valid_topics):
        self.valid_topics = valid_topics

    def upload_validation(self):
        """Validate the upload using general criteria

        Returns:
            tuple -- status code and a user friendly message
        """
        extra = log.get_extra(account=self.account, request_id=self.request_id)
        content_length = int(self.request.headers["Content-Length"])
        if content_length >= config.MAX_LENGTH:
            metrics.uploads_too_large.inc()
            logger.error("Payload too large. Request ID [%s] - Length %s", self.request_id, str(config.MAX_LENGTH), extra=extra)
            return self.error(413, f"Payload too large: {content_length}. Should not exceed {config.MAX_LENGTH} bytes", **extra)
        try:
            serv_dict = filechecks.get_service(self.payload_data['content_type'])
        except Exception:
            metrics.uploads_unsupported_filetype.inc()
            logger.exception("Unsupported Media Type: [%s] - Request-ID [%s]", self.payload_data['content_type'], self.request_id, extra=extra)
            return self.error(415, 'Unsupported Media Type', **extra)
        if serv_dict["service"] not in self.valid_topics:
            logger.error("Unsupported MIME type: [%s] - Request-ID [%s]", self.payload_data['content_type'], self.request_id, extra=extra)
            return self.error(415, 'Unsupported MIME type', **extra)

    def __get_metadata_from_request(self):
        if self.request.files.get('metadata'):
            return self.request.files['metadata'][0]['body'].decode('utf-8')
        elif self.request.body_arguments.get('metadata'):
            return self.request.body_arguments['metadata'][0].decode('utf-8')

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

    @prom_time(metrics.uploads_post_time)
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
        metrics.uploads_total.inc()
        self.request_id = self.request.headers.get('x-rh-insights-request-id', uuid.uuid4().hex)
        self.account = "unknown"

        # is this really ok to be optional?
        self.b64_identity = self.request.headers.get('x-rh-identity')
        if self.b64_identity:
            with metrics.uploads_json_loads.labels(key="post").time():
                header = json.loads(base64.b64decode(self.b64_identity))
            self.identity = header['identity']
            self.account = self.identity["account_number"]

        extra = log.get_extra(account=self.account, request_id=self.request_id)

        if not self.request.files.get('upload') and not self.request.files.get('file'):
            return self.error(
                415,
                "Upload field not found",
                files=list(self.request.files),
                **extra
            )

        try:
            upload_field = list(self.request.files)[0]
            metrics.uploads_file_field.labels(field=upload_field).inc()
        except IndexError:
            pass
        self.payload_data = self.request.files.get('upload')[0] if self.request.files.get('upload') else self.request.files.get('file')[0]

        if self.request_id is None:
            return self.error(400, "No request_id assigned.  Upload failed.", **extra)

        if self.upload_validation():
            metrics.uploads_invalid.inc()
        else:
            metrics.uploads_valid.inc()
            self.metadata = self.__get_metadata_from_request()
            service_dict = filechecks.get_service(self.payload_data['content_type'])
            self.service = service_dict["service"]
            self.category = service_dict["category"]
            self.size = int(self.request.headers['Content-Length'])
            body = self.payload_data['body']

            self.filedata = body

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


class VersionHandler(tornado.web.RequestHandler):
    """Handler for the `version` endpoint
    """
    def initialize(self, build_id, build_date):
        self.build_date = build_date
        self.build_id = build_id

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
        response = {'commit': self.build_id,
                    'date': self.build_date}
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
        self.write(metrics.generate_latest())


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
