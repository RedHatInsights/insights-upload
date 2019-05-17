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


from utils import mnm, config
from utils.storage import localdisk as local_storage, s3 as s3_storage
from tests.fixtures import StopLoopException


def prepare_app():
    file_path = "/tmp/topics.json"
    body = """
    [{
       'TOPIC_NAME': 'platform.upload.advisor',
       'PARTITIONS': 3,
       'REPLICAS': 3
     },
     {
       'TOPIC_NAME': 'platform.upload.testareno',
       'PARTITIONS': 3,
       'REPLICAS': 3
    }]
    """
    try:
        sh.rm(file_path)
    except Exception:
        pass

    with open(file_path, "w") as fp:
        fp.write(body)

    os.environ['TOPIC_CONFIG'] = '/tmp/topics.json'
    os.environ['OPENSHIFT_BUILD_COMMIT'] = '8d06f664a88253c361e61af5a4fa2ac527bb5f46'

    import app

    return app


app = prepare_app()


@pytest.fixture()
def s3_mocked():
    with mock_s3():
        client = boto3.client("s3")
        client.create_bucket(Bucket=s3_storage.PERM)
        client.create_bucket(Bucket=s3_storage.REJECT)
        s3_storage.s3 = client

        yield client


@pytest.fixture
def local_file():

    yield b"0" * 100


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
        "rh_account": "testing",
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
            raise StopLoopException('Stopping the iteration')
        elif _original_len == 0 and self._stop_iteration > 0:
            self._stop_iteration -= 1
        elif _original_len > 0:
            self._stop_iteration = 2

        return _original_len

    def __bool__(self):
        return self.__len__() != 0


@pytest.fixture
def produce_queue_mocked():
    orig_queue = app.produce_queue
    app.produce_queue = MyDeque([], 999)
    yield app.produce_queue
    app.produce_queue = orig_queue


@pytest.fixture
def broker_stage_messages(s3_mocked, produce_queue_mocked):
    def set_url(_file, service, avoid_produce_queue=False, validation='success'):
        file_name = uuid.uuid4().hex

        file_path = s3_storage.write(
            _file,
            s3_storage.PERM,
            file_name,
            config.DUMMY_VALUES['account'],
            'curl/7.61.1'
        )

        values = {
            'account': config.DUMMY_VALUES['account'],
            'principal': config.DUMMY_VALUES['principal'],
            'validation': validation,
            'request_id': file_name,
            'size': 100,
            'service': service,
            'url': file_path
        }

        if not avoid_produce_queue:
            produce_queue_mocked.append({'topic': 'platform.upload.' + service, 'msg': values})

        return values

    return set_url


@pytest.yield_fixture
def event_loop():
    # Set-up
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop

    # Clean-up
    loop.close()
