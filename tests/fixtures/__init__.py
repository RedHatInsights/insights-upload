import app
from os import getenv, path

UPLOAD_API_URL = getenv(
    'DEMO_UPLOAD',
    'http://upload-service-demo2.1b13.insights.openshiftapps.com'
)
UPLOAD_API_VERSION = "0.0.1"
UPLOAD_APP_TYPE = "application/vnd.redhat.advisor.test+tgz"
UPLOAD_ARCHIVE = "%s/resources/insights_sample.tar.gz" % path.join(path.dirname(__file__))

MIN_FILE_SIZE = 100
ALLOWED_MAX_LENGTH = app.MAX_LENGTH
FILE_SIZE_OVER_FLOW = 1024
