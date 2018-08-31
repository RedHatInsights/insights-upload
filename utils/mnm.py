import logging
import os

import requests

INFLUXDB_PLATFORM = os.getenv('INFLUX_URL', 'http://influxdb.mnm.svc.cluster.local:8086/write?db=platform')
INFLUX_USER = os.getenv('INFLUX_USER')
INFLUX_PASS = os.getenv('INFLUX_PASS')

NAMESPACE = 'unknown'
NAMESPACE_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'

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
    upload_stats,account_number={account},namespace={namespace} size={size}
    upload_stats,account_number={account},namespace={namespace} validation={validation}
    """.format(**values)

    try:
        r = requests.post(INFLUXDB_PLATFORM, auth=(INFLUX_USER, INFLUX_PASS), data=data)
        r.raise_for_status()
    except Exception as e:
        logger.info('Write to InfluxDB platform database failed:\n' + str(e))
