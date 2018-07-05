import boto3
import os

from botocore.exceptions import ClientError

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', None)

s3 = boto3.client('s3',
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def upload_to_s3(data, bucket_name, key):
    s3.upload_file(data, bucket_name, key)


def read_from_s3(bucket_name, key, filename):
    s3.download_file(bucket_name, key, filename)


def transfer(key, source_bucket, dest_bucket):
    copy_src = {'Bucket': source_bucket,
                'Key': key}
    s3.copy(copy_src, dest_bucket, copy_src['Key'])
    s3.delete_object(Bucket=source_bucket, Key=key)


def delete_object(key, bucket_name):
    s3.delete_object(Bucket=bucket_name, Key=key)


def object_info(key, bucket_name):
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError:
        return False


# placeholder since we might need this and i don't want to forget
def upload_to_azure():
    pass
