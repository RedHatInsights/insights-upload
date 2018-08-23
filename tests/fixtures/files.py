import pytest

from tests.fixtures import MIN_FILE_SIZE

from tests.util import run


@pytest.fixture
def local_file():
    file_name = _allocate_file(MIN_FILE_SIZE, 'insights_min.tar.gz')
    yield file_name

    cmd = "rm -f %s" % file_name
    run(cmd, sudo=False)


def _allocate_file(file_size, file_name):
    """
    Allocate file in /var/tmp
    """
    file_name = "/var/tmp/%s" % file_name

    cmd = "fallocate -l %s %s" % (file_size, file_name)
    rs = run(cmd, sudo=False)

    if rs["rc"] != 0:
        assert False, "Couldn't allocate file for testing"

    return file_name
