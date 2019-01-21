import logging
import os

import requests

from prometheus_client import Counter, Summary, generate_latest # noqa

INFLUXDB_PLATFORM = os.getenv('INFLUX_URL', 'http://influxdb.mnm.svc.cluster.local:8086/write?db=platform')
INFLUX_USER = os.getenv('INFLUX_USER')
INFLUX_PASS = os.getenv('INFLUX_PASS')

NAMESPACE = 'unknown'
NAMESPACE_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'

# Prometheus Counters
uploads_total = Counter('uploads_total', 'The total amount of uploads')
uploads_valid = Counter('uploads_valid', 'The total amount of valid uploads')
uploads_validated = Counter('uploads_validated_success', 'The total amount of successfully validated uploads')
uploads_invalid = Counter('uploads_invalid', 'Thte total number of invalid uploads')
uploads_invalidated = Counter('uploads_validated_failure', 'The total amount of uploads invalidated by services')
uploads_too_large = Counter('uploads_too_large', 'The total amount of uploads great than max_length')
uploads_unsupported_filetype = Counter('uploads_unsupported_filetype', 'The total amount of uploads not matching mimetype regex')

# Prometheus Summaries
uploads_write_tarfile = Summary('uploads_write_tarfile_seconds', 'Total seconds it takes to write the tarfile upon upload')
uploads_post_time = Summary('uploads_total_post_seconds', 'Total time it takes to post to upload service')
uploads_handle_file_seconds = Summary('uploads_handle_file_seconds', 'Total time to handle files once validated by end service')

logger = logging.getLogger(__name__)

# Get the namespace to tag for influxdb.
if os.path.exists(NAMESPACE_PATH):
    with open(NAMESPACE_PATH, 'r') as f:
        NAMESPACE = f.read().strip()


def send_to_influxdb(values):
    if not (INFLUX_USER and INFLUX_PASS):
        return

    values['namespace'] = NAMESPACE

    data = """
    upload_stats,account_number={rh_account},namespace={namespace} size={size}
    upload_stats,account_number={rh_account},namespace={namespace} validation={validation}
    """.format(**values)

    try:
        r = requests.post(INFLUXDB_PLATFORM, auth=(INFLUX_USER, INFLUX_PASS), data=data)
        r.raise_for_status()
    except Exception as e:
        logger.info('Write to InfluxDB platform database failed:\n' + str(e))
