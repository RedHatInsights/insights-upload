import boto3
import os

from botocore.exceptions import ClientError

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', None)

# S3 buckets
QUARANTINE = os.getenv('S3_QUARANTINE', 'insights-upload-quarantine')
PERM = os.getenv('S3_PERM', 'insights-upload-perm-test')
REJECT = os.getenv('S3_REJECT', 'insights-upload-rejected')

s3 = boto3.client('s3',
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def write(data, dest, uuid):
    s3.upload_file(data, dest, uuid)
    url = s3.generate_presigned_url('get_object',
                                    Params={'Bucket': dest,
                                            'Key': uuid}, ExpiresIn=100)
    return url


def copy(src, dest, uuid):
    copy_src = {'Bucket': src,
                'Key': uuid}
    s3.copy(copy_src, dest, uuid)
    s3.delete_object(Bucket=src, Key=uuid)
    url = s3.generate_presigned_url('get_object',
                                    Params={'Bucket': dest,
                                            'Key': uuid})
    return url


def ls(src, uuid):
    return s3.head_object(Bucket=src, Key=uuid)


def up_check(name):
    exists = True
    try:
        s3.head_bucket(Bucket=name)
    except ClientError as e:
        if int(e.response['Error']['Code']) == 404:
            exists = False

    return exists
