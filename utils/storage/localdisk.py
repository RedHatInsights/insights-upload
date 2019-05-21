import os

PERM = os.getenv('S3_PERM', 'insights-upload-perm-test')
REJECT = os.getenv('S3_REJECT', 'insights-upload-rejected')
WORKDIR = os.getenv('WORKDIR', '/tmp/uploads')
dirs = [WORKDIR,
        os.path.join(WORKDIR, PERM),
        os.path.join(WORKDIR, REJECT)]


def stage():
    for dir_ in dirs:
        os.makedirs(dir_, exist_ok=True)


def write(data, dest, uuid, account, user_agent):
    dir_path = os.path.join(WORKDIR, dest)
    if dir_path in dirs and not os.path.isdir(dir_path):
        stage()
    with open(os.path.join(dir_path, uuid), 'wb') as f:
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
