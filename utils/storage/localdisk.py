import os

QUARANTINE = os.getenv('S3_QUARANTINE', 'insights-upload-quarantine')
PERM = os.getenv('S3_PERM', 'insights-upload-perm-test')
REJECT = os.getenv('S3_REJECT', 'insights-upload-rejected')
WORKDIR = os.getenv('WORKDIR', '/tmp/uploads')
dirs = [WORKDIR,
        os.path.join(WORKDIR, QUARANTINE),
        os.path.join(WORKDIR, PERM),
        os.path.join(WORKDIR, REJECT)]


def stage():
    for dir_ in dirs:
        os.makedirs(dir_, exist_ok=True)


def write(data, dest, uuid):
    if not os.path.isdir(WORKDIR):
        stage()
    with open(os.path.join(WORKDIR, dest, uuid), 'w') as f:
        f.write(data)
        url = f
    return url.name


def ls(src, uuid):
    if os.path.isfile(os.path.join(WORKDIR, src, uuid)):
        return True


def copy(src, dest, uuid):
    os.rename(os.path.join(WORKDIR, src, uuid),
              os.path.join(WORKDIR, dest, uuid))
    return os.path.join(WORKDIR, dest, uuid)
