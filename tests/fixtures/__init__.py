import os
import shutil
import boto3
import pytest
import sh
from moto import mock_s3

from utils.storage import localdisk as local_storage, s3 as s3_storage


@pytest.fixture()
def s3_mocked():
    mock_s3().start()
    client = boto3.client("s3")

    client.create_bucket(Bucket=s3_storage.QUARANTINE)
    client.create_bucket(Bucket=s3_storage.PERM)

    s3_storage.s3 = client

    yield client

    mock_s3().stop()


@pytest.fixture
def local_file():
    """
    Allocate file in /var/tmp
    """
    file_path = "/var/tmp/insights.tar.gz"
    sh.fallocate("-l", "100", file_path)

    yield file_path

    sh.rm("-f", file_path)


@pytest.fixture(scope='function')
def with_local_folders():
    for _dir in local_storage.dirs:
        os.makedirs(_dir, exist_ok=True)

    yield None

    for _dir in local_storage.dirs:
        shutil.rmtree(_dir, ignore_errors=True)


@pytest.fixture(scope='function')
def no_local_folders():
    for _dir in local_storage.dirs:
        shutil.rmtree(_dir, ignore_errors=True)
