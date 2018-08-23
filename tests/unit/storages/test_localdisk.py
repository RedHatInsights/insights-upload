import hashlib
import shutil
import pytest
import uuid
import os

from utils.storage import localdisk as storage


class TestLocalDisk:

    def setup_method(self):
        self.temp_file_name = uuid.uuid4().hex
        self.quarantine_folder = 'insights-upload-quarantine'
        self.perm_folder = 'insights-upload-perm-test'
        self.non_existing_folder = 'some-random-folder'

        for _dir in storage.dirs:
            os.makedirs(_dir, exist_ok=True)

    def teardown_method(self):
        for _dir in storage.dirs:
            shutil.rmtree(_dir, ignore_errors=True)

    def test_write(self):
        file_name = storage.write(os.urandom(100).decode('latin1'), self.quarantine_folder, self.temp_file_name)

        assert self.temp_file_name == os.path.basename(file_name)
        assert os.path.isfile(file_name)

    def test_write_wrong_destination(self):
        with pytest.raises(FileNotFoundError):
            storage.write(os.urandom(100).decode('latin1'), self.non_existing_folder, self.temp_file_name)

    def test_write_no_folders_at_all(self):
        self.teardown_method()
        file_name = storage.write(os.urandom(100).decode('latin1'), self.quarantine_folder, self.temp_file_name)

        assert self.temp_file_name == os.path.basename(file_name)
        assert os.path.isfile(file_name)

    def test_ls(self):
        storage.write(os.urandom(100).decode('latin1'), self.quarantine_folder, self.temp_file_name)
        assert storage.ls(self.quarantine_folder, self.temp_file_name) is True

    def test_ls_file_not_found(self):
        assert storage.ls(self.quarantine_folder, self.temp_file_name) is None

    def test_stage(self):
        # just to make sure that there is no folder left in there
        self.teardown_method()
        storage.stage()

        for _dir in storage.dirs:
            assert os.path.isdir(_dir) is True

    def test_copy(self):
        original_file_path = storage.write(os.urandom(100).decode('latin1'), self.quarantine_folder, self.temp_file_name)

        original_file = open(original_file_path, 'rb')
        original_checksum = hashlib.md5(original_file.read()).hexdigest()
        original_file.close()

        copied_file_path = storage.copy(self.quarantine_folder, self.perm_folder, self.temp_file_name)

        assert os.path.basename(original_file_path) == os.path.basename(copied_file_path)
        assert original_file_path != copied_file_path

        with pytest.raises(FileNotFoundError):
            open(original_file_path, 'rb')

        copied_file = open(copied_file_path, 'rb')

        # Checksum confirmation!
        assert original_checksum == hashlib.md5(copied_file.read()).hexdigest()
        copied_file.close()
