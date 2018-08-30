import asyncio
import collections
import os
import shutil
import uuid
from importlib import reload

import boto3
import pytest
import responses
import sh
from moto import mock_s3

import app
from utils import mnm
from utils.storage import localdisk as local_storage, s3 as s3_storage


@pytest.fixture()
def s3_mocked():
    with mock_s3():
        client = boto3.client("s3")
        client.create_bucket(Bucket=s3_storage.QUARANTINE)
        client.create_bucket(Bucket=s3_storage.PERM)
        s3_storage.s3 = client

        yield client


@pytest.fixture
def local_file():
    """
    Allocate file in /var/tmp
    """
    file_path = "/tmp/insights.tar.gz"
    try:
        sh.rm(file_path)
    except Exception as e:
        pass

    sh.fallocate("-l", "100", file_path)

    yield file_path

    sh.rm(file_path)


@pytest.fixture
def with_local_folders():
    for _dir in local_storage.dirs:
        os.makedirs(_dir, exist_ok=True)

    yield None

    for _dir in local_storage.dirs:
        shutil.rmtree(_dir, ignore_errors=True)


@pytest.fixture
def no_local_folders():
    for _dir in local_storage.dirs:
        shutil.rmtree(_dir, ignore_errors=True)


@responses.activate
@pytest.fixture
def influx_db_mock():
    # responses.reset()
    mnm.INFLUXDB_PLATFORM = 'http://some.influx.endpoint.com/write?db=platform'
    responses.add(
        responses.POST, mnm.INFLUXDB_PLATFORM,
        json={"message": "saved"}, status=201
    )

    yield mnm.INFLUXDB_PLATFORM
    mnm.INFLUXDB_PLATFORM = os.getenv('INFLUXDB_PLATFORM')


@responses.activate
@pytest.fixture
def influx_db_error_mock():
    # responses.reset()
    mnm.INFLUXDB_PLATFORM = 'http://some.influx.endpoint.com/write?db=platform'
    responses.add(
        responses.POST, mnm.INFLUXDB_PLATFORM,
        json={"message": 'error'}, status=422
    )

    yield mnm.INFLUXDB_PLATFORM
    mnm.INFLUXDB_PLATFORM = os.getenv('INFLUXDB_PLATFORM')


@pytest.fixture
def influx_db_values():
    yield {
        "account": "testing",
        "size": "123",
        "validation": "is_valid"
    }


@pytest.fixture
def influx_db_credentials():
    mnm.INFLUX_USER = 'someuser'
    mnm.INFLUX_PASS = 'somepass'

    yield

    mnm.INFLUX_USER = os.getenv('INFLUX_USER')
    mnm.INFLUX_PASS = os.getenv('INFLUX_PASS')


@pytest.fixture
def influx_db_namespace():
    os.system("sudo mkdir -m 0777 -p {}".format(os.path.dirname(app.mnm.NAMESPACE_PATH)))
    os.system("sudo chmod 0777 {}".format(os.path.dirname(app.mnm.NAMESPACE_PATH)))
    os.system("sudo echo 'somenamespace' > {}".format(app.mnm.NAMESPACE_PATH))
    reload(mnm)
    yield
    os.system("sudo rm -Rf {}".format(os.path.dirname(app.mnm.NAMESPACE_PATH)))


class MyDeque(collections.deque):
    _stop_iteration = 2

    def __init__(self, *args, **kwargs):
        super(MyDeque, self).__init__(*args, **kwargs)

    def __len__(self):
        _original_len = super(MyDeque, self).__len__()
        if _original_len == 0 and self._stop_iteration == 0:
            self._stop_iteration = 2
            raise Exception('Stopping the iteration')
        elif _original_len == 0 and self._stop_iteration > 0:
            self._stop_iteration -= 1
        elif _original_len > 0:
            self._stop_iteration = 2

        return _original_len


@pytest.fixture
def broker_stage_messages(s3_mocked):
    app.produce_queue = MyDeque([], 999)

    def set_url(_file, service, avoid_produce_queue=False, validation='success'):
        file_name = uuid.uuid4().hex

        file_path, _ = s3_storage.write(
            _file,
            s3_storage.QUARANTINE,
            file_name
        )

        values = {
            'rh_account': app.DUMMY_VALUES['rh_account'],
            'principal': app.DUMMY_VALUES['principal'],
            'validation': validation,
            'hash': file_name,
            'size': 100,
            'service': service,
            'url': file_path
        }

        if not avoid_produce_queue:
            app.produce_queue.append({'topic': service, 'msg': values})

        return values

    yield set_url

    app.produce_queue = collections.deque([], 999)


@pytest.yield_fixture
def event_loop():
    # Set-up
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop

    # Clean-up
    loop.close()
