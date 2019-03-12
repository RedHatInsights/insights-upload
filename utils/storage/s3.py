import os
import asyncio
import aioboto3
import boto3

from utils import mnm

from tornado.ioloop import IOLoop
from botocore.exceptions import ClientError
from prometheus_async.aio import time as prom_time


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", None)
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", None)
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", None)

# S3 buckets
QUARANTINE = os.getenv("S3_QUARANTINE", "insights-upload-quarantine")
PERM = os.getenv("S3_PERM", "insights-upload-perm-test")
REJECT = os.getenv("S3_REJECT", "insights-upload-rejected")

s3block = boto3.client("s3",
                       endpoint_url=S3_ENDPOINT_URL,
                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

def get_client():
    return aioboto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


class Client(object):

    async def __aenter__(self):
        self.client = get_client()
        return self.client

    async def __aexit__(self, exc_type, exc, tb):
        await self.client.close()

@prom_time(mnm.uploads_s3_write_seconds)
async def write(data, dest, uuid):
    async with Client() as s3:
        await s3.upload_file(data, dest, uuid)
        url = s3.generate_presigned_url(
            "get_object", Params={"Bucket": dest,
                                  "Key": uuid}, ExpiresIn=86400
        )
    return url


@prom_time(mnm.uploads_s3_copy_seconds)
async def copy(src, dest, uuid):
    copy_src = {"Bucket": src, "Key": uuid}
    async with Client() as s3:
        await s3.copy(copy_src, dest, uuid)
        await s3.delete_object(Bucket=src, Key=uuid)
        url = s3.generate_presigned_url(
            "get_object", Params={"Bucket": dest,
                                  "Key": uuid}, ExpiresIn=86400
        )
    return url


@mnm.uploads_s3_ls_seconds.time()
def ls(src, uuid):
    try:
        result = s3block.head_object(Bucket=src, Key=uuid)
        return result
    except ClientError:
        return {"ResponseMetadata": {"HTTPStatusCode": 404}}


async def up_check(name):
    exists = True
    try:
        async with Client() as s3:
            await s3.head_bucket(Bucket=name)
    except ClientError as e:
        if int(e.response["Error"]["Code"]) == 404:
            exists = False

    return exists
