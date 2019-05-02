import os
import sys
import logging
import uuid
from . import config

from logstash_formatter import LogstashFormatterV1
from boto3.session import Session
import watchtower

container = str(uuid.uuid4())


class ContextFilter(logging.Filter):

    def filter(self, record):
        record.container = container
        return True


def setup_logging():
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
    other_loggers = [logging.getLogger(n) for n in (
        'tornado.general',
        'tornado.application',
        'kafkahelpers',
    )]
    for l in other_loggers:
        l.setLevel('ERROR')

    for l in (logger, *other_loggers):
        l.addFilter(ContextFilter())

    NAMESPACE = config.get_namespace()

    if (config.CW_AWS_ACCESS_KEY_ID and config.CW_AWS_SECRET_ACCESS_KEY):
        CW_SESSION = Session(aws_access_key_id=config.CW_AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=config.CW_AWS_SECRET_ACCESS_KEY,
                             region_name=config.CW_AWS_REGION_NAME)
        cw_handler = watchtower.CloudWatchLogHandler(boto3_session=CW_SESSION,
                                                     log_group="platform",
                                                     stream_name=NAMESPACE)
        cw_handler.setFormatter(LogstashFormatterV1())
        logging.root.addHandler(cw_handler)
