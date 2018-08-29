import boto3
import pytest
from moto import mock_s3
from utils.storage import s3


@pytest.fixture()
def s3_mocked():
    mock_s3().start()
    client = boto3.client("s3")

    client.create_bucket(Bucket=s3.QUARANTINE)
    client.create_bucket(Bucket=s3.PERM)

    yield client

    mock_s3().stop()
