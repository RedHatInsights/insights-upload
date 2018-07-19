import os

os.makedirs('/tmp/uploads')
WORKDIR = '/tmp/uploads'


def write(data, dest, uuid):
    with open(WORKDIR + '/' + dest + '/' + uuid, 'w') as f:
        f.write(data)


def ls(src, uuid):
    if os.path.isfile(WORKDIR + '/' + src + '/' + uuid):
        return True
    else:
        return False


def copy(src, dest, uuid):
    os.rename(WORKDIR + '/' + src + '/' + uuid,
              WORKDIR + '/' + dest + '/' + uuid)
    return WORKDIR + '/' + dest + '/' + uuid
