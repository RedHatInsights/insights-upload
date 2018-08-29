# Insights Platform Upload Service

The Upload Service is designed to ingest payloads from customers and distribute
them via a message queue to other Platform services.

## Details

The upload service is a component of Insights Platform that allows for customers
to upload data to Red Hat. The service sits behind a 3Scale API gateway that
handles authentication and routing. It runs on Tornado 5 and Python 3.6.

The upload service has an interface into Insights S3 Buckets for the storage of
customer data. It also connects to the Insights Message Bus in order to notify
services of new and available payloads for processing.

The service runs in Openshift Dedicated.

## How it Works

The upload service workflow is as follows:

    client > upload service > service topic on the MQ > validating service >
    uploadvalidation topic on the MQ with result >
    URL for permanent file location added to `available` topic on the MQ >
    Other service consume the `available` topic

The key here for most services is to understand that in order to be notified
of new, validated payloads, they **must** subscribe to the `available` topic on the message
queue.

### Errors

The upload service will report back to the client HTTP errors if something goes
wrong. It will be the responsibility of the client to communicate that connection
problem back to the user via a log message or some other means.

## Getting Started

The local development environment is configured using docker and docker-compose.
The docker-compose file included in this repo will stand up a message queue, the
upload-service app, and a consumer for a test queue. It is currently configured
to simply fail all validations and send them to the rejected bucket. For testing,
this should be fine.

### Prequisites

    docker
    docker-compose
    AWS credentials with S3 access

By default, the app will use the insights S3 account. If you do not have access
to this, you will need to provide your own AWS creds and buckets via environment
variables OR choose to use localdisk rather than S3 by changing the code in app.py:

    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY

    # buckets (3 required for proper operation)
    S3_QUARANTINE
    S3_PERM
    S3_REJECT
    
Or change the import to localdisk:

    from utils import localdisk as storage

Also, you need to add the following environment variable, in order to run the tests

    ASYNC_TEST_TIMEOUT=10

### Installing

Once your environment variables are set on your localhost, bring up the stack:

    LINUX
    cd ./docker && sh startup.sh

    WINDOWS
    cd .\docker
    .\startup.ps1
    
This will stand up the full stack as well as initialize the topics in the message 
queue that are necessary for testing. The Kiel library does not automatically create 
topics in the MQ when they do not exist, so created them is critical.

Upload a file to test to see if the system is working properly. Any file will
work in testing as long as the `type` field is set properly. Use the `README.md`
file in this repo if you'd like.

    curl -vvvv -F "upload=@test-archive.tar.gz;type=application/vnd.redhat.testareno.something+tgz" localhost:8080/api/v1/upload

**NOTE**: The service **testareno** is important for local testing as it's the service queue
that our test consumer is listening to.

You should see messages in the docker logs where the upload-service sends a message,
the consumer picks it up, returns a failure message, then the upload-service sends it to
the permanent bucket.

To see the docker-compose logs:

    sudo docker-compose logs -f

## Running with Tests

Any new features added to the application should be accompanied by a Unittest/Pytest in `./tests`

To test the app, activate the virtualenv and then run pytest and flake8.

    source bin/activate
    pytest
    flake8

There is several ways to generate the coverage report, but the commonly ways are:
    
    1. pytest --cov=.
    2. pytest --cov=. --cov-report html
    
**NOTE**: you will find the HTML report at `./htmlcov`

For information on Tornado testing, see [the documentation](http://www.tornadoweb.org/en/stable/_modules/tornado/testing.html)

## Deployment

The upload service `master` branch has a webhook that notifies the Openshift Dedicated
cluster to build a new image. This image is immediately deployed in the Platform-CI project.
If this image is tested valid and operational, it should be tagged to QA for further testing,
and then finally tagged to `Production`.

**WIP** - the QA and Production projects are not in place yet. The project is deployed on
`Platform-CI` only

## Contributing

All outstanding issues or feature requests should be filed as Issues on this Github
page. PRs should be submitted against the master branch for any new features or changes.

## Versioning

Anytime an endpoint is modified, the versin should be incremented by `0.1`. New
functionality introduced that may effect the client should increment by `1`. Minor
features and bug fixes can increment by `0.0.1`

## Authors

* **Stephen Adams** - **Initial Work** - [SteveHNH](https://github.com/SteveHNH)
