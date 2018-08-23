import uuid
import os

from utils.storage import s3 as storage
from tests.fixtures.s3 import s3_mocked # noqa
from tests.fixtures.files import local_file


class TestS3:

    def test_credentials_acl(self):
        for bucket in (storage.QUARANTINE, storage.PERM, storage.REJECT):
            credentials = storage.s3.get_bucket_acl(Bucket=bucket)
            assert credentials['Grants'][0]['Permission'], 'FULL_CONTROL'

    def test_write(self, local_file, s3_mocked, key_name=None):
        storage.s3 = s3_mocked

        key_name = uuid.uuid4().hex if not key_name else key_name

        s3_response = storage.write(
            local_file,
            storage.QUARANTINE,
            key_name
        )

        assert s3_response is not None
        assert storage.QUARANTINE in s3_response

        return s3_response

    def test_copy(self, local_file, s3_mocked):
        storage.s3 = s3_mocked

        key_name = uuid.uuid4().hex

        write_response = self.test_write(local_file, storage.s3, key_name=key_name)
        copy_response = storage.copy(storage.QUARANTINE, storage.PERM, key_name)

        def _get_key(r):
            k = r.split('/')[3]
            return k[:k.find('?')]

        assert write_response is not None
        assert storage.QUARANTINE in write_response

        assert copy_response is not None
        assert storage.PERM in copy_response

        assert copy_response != write_response

        write_key, copy_key = _get_key(write_response), _get_key(copy_response)
        assert write_key == copy_key

    def test_ls(self, local_file, s3_mocked):
        storage.s3 = s3_mocked

        key_name = uuid.uuid4().hex
        write_response = self.test_write(local_file, storage.s3, key_name=key_name)

        ls_response = storage.ls(storage.QUARANTINE, key_name)

        assert write_response is not None
        assert ls_response is not None

        assert ls_response['ContentLength'] == os.stat(local_file).st_size
        assert ls_response['ResponseMetadata']['HTTPStatusCode'] == 200

    def test_up_check(self, s3_mocked):
        storage.s3 = s3_mocked

        assert storage.up_check(storage.QUARANTINE) is True
        assert storage.up_check('SomeBucket') is False
