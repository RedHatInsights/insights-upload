import os

QUARANTINE = os.getenv('S3_QUARANTINE', 'insights-upload-quarantine')
PERM = os.getenv('S3_PERM', 'insights-upload-perm-test')
REJECT = os.getenv('S3_REJECT', 'insights-upload-rejected')
WORKDIR = '/tmp/uploads/'
dirs = [WORKDIR,
        WORKDIR + 'insights-upload-quarantine',
        WORKDIR + 'insights-upload-perm-test',
        WORKDIR + 'insights-upload-rejected']


def stage():
    for dir_ in dirs:
        os.makedirs(dir_)


def write(data, dest, uuid):
    if not os.path.isdir('/tmp/uploads'):
        stage()
    with open('/tmp/uploads/' + dest + '/' + uuid, 'w') as f:
        f.write(data)
        url = f
    return url.name


def ls(src, uuid):
    if os.path.isfile('/tmp/uploads/' + src + '/' + uuid):
        return True


def copy(src, dest, uuid):
    os.rename('/tmp/uploads/' + src + '/' + uuid,
              '/tmp/uploads/' + dest + '/' + uuid)
    return '/tmp/uploads/' + dest + '/' + uuid
