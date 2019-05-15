import os
import logging
import json

import requests

from apispec import APISpec
from apispec_webframeworks.tornado import TornadoPlugin

logger = logging.getLogger('upload-service')

# Cloudwatch Logging
CW_AWS_ACCESS_KEY_ID = os.getenv('CW_AWS_ACCESS_KEY_ID', None)
CW_AWS_SECRET_ACCESS_KEY = os.getenv('CW_AWS_SECRET_ACCESS_KEY', None)
CW_AWS_REGION_NAME = os.getenv('CW_AWS_REGION_NAME', 'us-east-1')

# set max length to 10.5 MB (one MB larger than peak)
MAX_LENGTH = int(os.getenv('MAX_LENGTH', 11010048))
LISTEN_PORT = int(os.getenv('LISTEN_PORT', 8888))
RETRY_INTERVAL = int(os.getenv('RETRY_INTERVAL', 5))  # seconds

# Maximum workers for threaded execution
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 50))

# Maximum time to wait for an archive to upload to storage
STORAGE_UPLOAD_TIMEOUT = int(os.getenv('STORAGE_UPLOAD_TIMEOUT', 60))

VALIDATION_QUEUE = os.getenv('VALIDATION_QUEUE', 'platform.upload.validation')

STORAGE_DRIVER = os.getenv("STORAGE_DRIVER", "s3")

PATH_PREFIX = os.getenv('PATH_PREFIX', '/api/')
APP_NAME = os.getenv('APP_NAME', 'ingress')
API_PREFIX = PATH_PREFIX + APP_NAME

# Message Queue
MQ = os.getenv('KAFKAMQ', 'kafka:29092').split(',')
MQ_GROUP_ID = os.getenv('MQ_GROUP_ID', 'upload')

BUILD_ID = os.getenv('OPENSHIFT_BUILD_COMMIT', '8d06f664a88253c361e61af5a4fa2ac527bb5f46')

TOPIC_CONFIG = os.getenv('TOPIC_CONFIG', '/tmp/topics.json')
MAX_RECORDS = int(os.getenv("MAX_RECORDS", 1))

LOG_GROUP = os.getenv("LOG_GROUP", "platform")

# Items in this map are _special cases_ where the service cannot be extracted
# from the Content-Type
SERVICE_MAP = {
    'application/x-gzip; charset=binary': {
        'service': 'advisor',
        'category': 'upload'
    },
    'application/vnd.redhat.openshift.periodic': {
        'service': 'buckit',
        'category': 'openshift'
    }
}


# dummy values for testing without a real identity
DUMMY_VALUES = {
    'principal': 'default_principal',
    'account': '000001',
    'payload_id': '1234567890abcdef',
    'url': 'http://defaulttesturl',
    'validation': 0,
    'size': 0
}

# Set up ApiSpec object
spec = APISpec(
    title='Insights Upload Service',
    version='0.0.1',
    openapi_version='3.0.0',
    info=dict(
        description='A service designed to ingest payloads from customers and distribute them via message queue to other platform services.',
        contact=dict(
            email='sadams@redhat.com'
        )
    ),
    plugins=[
        TornadoPlugin()
    ]
)


def get_namespace():
    namespace = None
    try:
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
            namespace = f.read()
    except EnvironmentError:
        logger.info("Not Running on Openshift")
    return namespace


def get_valid_topics():
    VALID_TOPICS = []
    try:
        with open(TOPIC_CONFIG, 'r') as f:
            data = f.read().replace("'", '"')
            topic_config = json.loads(data)

        for topic in topic_config:
            for name in topic['TOPIC_NAME'].split('.'):
                VALID_TOPICS.append(name)
        return VALID_TOPICS
    except Exception:
        logger.exception("Unable to open topics.json. Using default topics")
        VALID_TOPICS = ["advisor",
                        "validation",
                        "testareno",
                        "hccm",
                        "compliance",
                        "qpc"]
        return VALID_TOPICS


def get_commit_date(commit_id):
    if os.getenv("GITHUB_ACCESS_TOKEN"):
        headers = {"Authorization": "token %s" % os.getenv("GITHUB_ACCESS_TOKEN")}
    else:
        headers = {}
    BASE_URL = "https://api.github.com/repos/RedHatInsights/insights-upload/git/commits/"
    try:
        response = requests.get(BASE_URL + commit_id, headers=headers)
        date = response.json()['committer']['date']
    except Exception:
        logger.exception("Unable to get commit date")
        date = "unknown"

    return date


VALID_TOPICS = get_valid_topics()
