import hashlib
import os
import uuid
from mock import patch
from random import randint
from string import ascii_letters, digits

import pytest
import responses
from botocore.exceptions import ClientError

from utils import mnm
from utils.storage import localdisk as local_storage, s3 as s3_storage


class TestS3(object):

    @pytest.mark.withoutresponses
    def test_credentials_acl(self):
        try:
            for bucket in (s3_storage.QUARANTINE, s3_storage.PERM, s3_storage.REJECT):
                credentials = s3_storage.s3.get_bucket_acl(Bucket=bucket)
                assert credentials['Grants'][0]['Permission'], 'FULL_CONTROL'
        except ClientError:
            pytest.xfail("Something is wrong with the AWS Credentials, please check them and run this test again")

    def test_write(self, local_file, s3_mocked):
        key_name = uuid.uuid4().hex

        url = s3_storage.write(
            local_file,
            s3_storage.QUARANTINE,
            key_name
        )

        assert url is not None
        assert isinstance(url, str)
        assert s3_storage.QUARANTINE in url

    def test_copy(self, local_file, s3_mocked):
        key_name = uuid.uuid4().hex

        write_file_path = s3_storage.write(
            local_file,
            s3_storage.QUARANTINE,
            key_name
        )
        copy_file_path = s3_storage.copy(s3_storage.QUARANTINE, s3_storage.PERM, key_name)

        def _get_key(r):
            k = r.split('/')[3]
            return k[:k.find('?')]

        assert isinstance(write_file_path, str)
        assert s3_storage.QUARANTINE in write_file_path
        assert copy_file_path is not None
        assert s3_storage.PERM in copy_file_path
        assert copy_file_path != write_file_path

        write_key, copy_key = _get_key(write_file_path), _get_key(copy_file_path)
        assert write_key == copy_key

    def test_ls(self, local_file, s3_mocked):
        key_name = uuid.uuid4().hex
        file_url = s3_storage.write(
            local_file,
            s3_storage.QUARANTINE,
            key_name
        )

        ls_response = s3_storage.ls(s3_storage.QUARANTINE, key_name)

        assert file_url is not None
        assert isinstance(ls_response, dict)

        assert ls_response['ContentLength'] == os.stat(local_file).st_size
        assert ls_response['ResponseMetadata']['HTTPStatusCode'] == 200

    def test_up_check(self, s3_mocked):
        assert s3_storage.up_check(s3_storage.QUARANTINE) is True
        assert s3_storage.up_check('SomeBucket') is False

    def test_ls_not_found(self, local_file, s3_mocked):
        key_name = uuid.uuid4().hex

        with pytest.raises(ClientError) as e:
            s3_storage.ls(s3_storage.QUARANTINE, key_name)

        assert str(e.value) == 'An error occurred (404) when calling the HeadObject operation: Not Found'


class TestLocalDisk(object):

    @staticmethod
    def _get_file_data():
        return ''.join([(ascii_letters + digits)[randint(0, 61)] for _ in range(100)])

    def setup_method(self):
        self.temp_file_name = uuid.uuid4().hex
        self.non_existing_folder = 'some-random-folder'

    def test_write(self, with_local_folders):
        file_name = local_storage.write(self._get_file_data(), local_storage.QUARANTINE, self.temp_file_name)

        assert self.temp_file_name == os.path.basename(file_name)
        assert os.path.isfile(file_name)

    def test_write_wrong_destination(self, with_local_folders):
        with pytest.raises(FileNotFoundError):
            local_storage.write(self._get_file_data(), self.non_existing_folder, self.temp_file_name)

    def test_write_no_folders_at_all(self, no_local_folders):
        file_name = local_storage.write(self._get_file_data(), local_storage.QUARANTINE, self.temp_file_name)

        assert self.temp_file_name == os.path.basename(file_name)
        assert os.path.isfile(file_name)

    @patch("utils.storage.localdisk.open")
    @patch("utils.storage.localdisk.os.path.isdir", return_value=True)
    def test_write_return(self, isdir, open_mock):
        """
        Write method returns a file name
        """
        result = local_storage.write(self._get_file_data(), local_storage.QUARANTINE, self.temp_file_name)
        assert result == open_mock.return_value.__enter__.return_value.name

    def test_ls(self, with_local_folders):
        local_storage.write(self._get_file_data(), local_storage.QUARANTINE, self.temp_file_name)
        assert local_storage.ls(local_storage.QUARANTINE, self.temp_file_name) is True

    def test_ls_file_not_found(self, with_local_folders):
        assert local_storage.ls(local_storage.QUARANTINE, self.temp_file_name) is None

    def test_stage(self, no_local_folders):
        # just to make sure that there is no folder left in there
        local_storage.stage()

        for _dir in local_storage.dirs:
            assert os.path.isdir(_dir) is True

    def test_copy(self, with_local_folders):
        original_file_path = local_storage.write(self._get_file_data(), local_storage.QUARANTINE, self.temp_file_name)

        original_file = open(original_file_path, 'rb')
        original_checksum = hashlib.md5(original_file.read()).hexdigest()
        original_file.close()

        copied_file_path = local_storage.copy(local_storage.QUARANTINE, local_storage.PERM, self.temp_file_name)

        assert os.path.basename(original_file_path) == os.path.basename(copied_file_path)
        assert original_file_path != copied_file_path

        with pytest.raises(FileNotFoundError):
            open(original_file_path, 'rb')

        copied_file = open(copied_file_path, 'rb')

        # Checksum confirmation!
        assert original_checksum == hashlib.md5(copied_file.read()).hexdigest()
        copied_file.close()


class TestInfluxDB(object):

    @responses.activate
    @patch("utils.mnm.INFLUXDB_PLATFORM", "http://some.influx.endpoint.com/write?db=platform")
    def test_send_to_influxdb(self, influx_db_values):
        responses.add(
            responses.POST, mnm.INFLUXDB_PLATFORM,
            json={"message": "saved"}, status=201
        )

        old_user, old_pass = mnm.INFLUX_USER, mnm.INFLUX_PASS
        mnm.INFLUX_USER = mnm.INFLUX_PASS = "test"

        try:
            method_response = mnm.send_to_influxdb(influx_db_values)

            assert method_response is None
            assert len(responses.calls) == 1
            assert responses.calls[0].response.text == '{"message": "saved"}'
        finally:
            mnm.INFLUX_USER, mnm.INFLUX_PASS = old_user, old_pass

    @responses.activate
    @patch("utils.mnm.INFLUXDB_PLATFORM", "http://some.influx.endpoint.com/write?db=platform")
    def test_send_to_influxdb_no_credentials(self, influx_db_values):
        old_user, old_pass = mnm.INFLUX_USER, mnm.INFLUX_PASS
        mnm.INFLUX_USER = mnm.INFLUX_PASS = None

        responses.add(
            responses.POST, mnm.INFLUXDB_PLATFORM,
            json={"message": "saved"}, status=201
        )

        try:
            method_response = mnm.send_to_influxdb(influx_db_values)

            assert method_response is None
            assert len(responses.calls) == 0
        finally:
            mnm.INFLUX_USER, mnm.INFLUX_PASS = old_user, old_pass

    @responses.activate
    @patch("utils.mnm.INFLUXDB_PLATFORM", "http://some.influx.endpoint.com/write?db=platform")
    def test_send_to_influxdb_down(self, influx_db_values):
        responses.add(
            responses.POST, mnm.INFLUXDB_PLATFORM,
            json={"message": "error"}, status=201
        )

        old_user, old_pass = mnm.INFLUX_USER, mnm.INFLUX_PASS
        mnm.INFLUX_USER = mnm.INFLUX_PASS = "test"

        try:
            method_response = mnm.send_to_influxdb(influx_db_values)

            assert method_response is None
            assert len(responses.calls) == 1
            assert responses.calls[0].response.text == '{"message": "error"}'
        finally:
            mnm.INFLUX_USER, mnm.INFLUX_PASS = old_user, old_pass
