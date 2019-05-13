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

  - The source client sends a payload of a specific MIME type to the upload service
  - The upload service discovers the validating service via the MIME type, uploads
  it to a temporary S3 bucket and puts a message on the message queue in the format
  defined below
  - The validating service checks that the payload is safe and properly formatted
  - The validating service returns a message via the `platform.upload.validation` queue to the
  upload service with a failure or success message
  - If the validation succeeds, the upload service puts the payload on a permanent
  S3 bucket, and puts a message on the `available` queue notifying services that
  a new upload is available
  - If the validation fails, the upload service puts the payload on a rejected
  S3 bucket. This is available for diagnosis later in the event it is needed.

The key here for most services is to understand that in order to be notified
of new, validated payloads, they **must** subscribe to the `available` topic on the message
queue.

### Message Format

The message from the upload service is JSON as seen below:

    {'account': '123456', 'rh_account': '123456', 'principal': 'test_org', 'request_id': '52df9f748eabcfea', 'size': 356, 'service': 'testareno', 'b64_identity': '<identity header base64 string>', 'metadata': {'some_key': 'some_value', 'some_other_key': 'some_other_value'}, 'url': '/tmp/uploads/insights-upload-quarantine/52df9f748eabcfea'}

Fields:

  - account: The account number used to upload. Can be used to separate data for tenancy purposes.
  - rh_account: legacy support for the above value. To be deprecated
  - principal:  The uploading org id
  - request_id: Unique ID provided to the payload created by 3Scale. This ID will be used for the life of the object.
  - size:       Size of the payload in bytes
  - service:    The name of the service to do the validation
  - b64_identity: The base64 string from `x-rh-identity` header
  - metadata: Optional keys and values sent from the client
  - url:        URL for the location the payload can be downloaded from

Principal is currently reflecting the org_id of the account, though that may change
as we understand what is most useful regarding who uploaded a particular archive. The request_id
is a unique ID assigned to the uploaded file by the 3Scale gateway. Everything else
is fairly self-explanatory.

Service should return most of the same data that was received with the addition of the `validation` key indicated success or failure of the object. They will return this data to the `platform.upload.validation` topic.

This is what a message with **minimum** required data should look like:

    {'request_id': '52df9f748eabcfea', 'service': 'advisor', 'validation': 'success'} # or 'validation': 'failure'

Fields:

  - request_id: Unique ID being addresed by validation message
  - service: The service that performed the validation
  - validation: Either succes or failure based on whether the payload passed validation or not

Optional Fields. These should be returned if at all possible for use in the available topic

  - account: The account number from the processed payload
  - principal: The principal from the processed payload
  - b64_identity: The identity header.
  - id: If inventory was posted to, return this id so other services can cross reference that data

### Current Active Topics

The following topics are currently in use in the MQ service:

  - platform.upload.advisor             # for the advisor service
  - platform.upload.testareno           # for testing the mq to upload service connection
  - platform.upload.validation    # for responses from validation services
  - platform.upload.available           # for new uploads available to other services
  - platform.upload.hccm                # HCCM service
  - platform.upload.compliance          # Compliance service
  - platform.upload.qpc                 # QPC service

### Errors

The upload service will report back to the client HTTP errors if something goes
wrong with the initial upload. It will be the responsibility of the client to communicate that connection
problem back to the user via a log message or some other means.

## Getting Started

The local development environment can be either configured using docker and docker-compose,
or by pointing the upload-service and consumer apps to an existing kafka server.
The test queue consumer app is currently configured to simply fail all validations and send
them to the rejected bucket. For testing, this should be fine.

### Docker

The docker-compose file included in this repo will stand up a message queue, the
upload-service app, and a consumer for a test queue.

#### Prequisites

    docker
    docker-compose

By default, the app will use [Minio.io](https://minio.io), as a S3 backend compatible
with AWS' API.

If you prefer to use a real S3 account, you will need to remove the environment variable
S3_ENDPOINT_URL in ['docker/docker-compose.yml'](docker/docker-compose.yml) and provide
your own AWS creds and buckets via environment variables:

    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY

    # buckets (3 required for proper operation)
    S3_QUARANTINE
    S3_PERM
    S3_REJECT

Another option is to use localdisk rather than S3 by setting an environment variable:

    STORAGE_DRIVER=localdisk

Also, you need to add the following environment variable, in order to run the tests

    ASYNC_TEST_TIMEOUT=10

#### Installing

Once your environment variables are set on your localhost, bring up the stack. You
may need to be root depending on your environment.

    cd ./docker && docker-compose up -d

This will stand up the full stack. You can follow logs in docker-compose with
`docker-compose logs -f`. By default, three buckets called 'insights-upload-perm-test',
'insights-upload-quarantine', 'insights-upload-rejected' will be created.
You may visit [http://localhost:9000](http://localhost:9000) to check the contents or
create new buckets as you need.

### Bare metal

It’s possible to run the apps manually and make them connect to an existing Kafka instance.

#### Prerequisities

    python3 (preferrably 3.6) with venv
    zookeeper
    kafka
    minio or s3 credentials

##### Queue

You can either connect to a remote kafka server, or set up a local one. To spawn your own
Kafka server, simply install it using your favorite package manager. To run Kafka, you need
Zookeeper and JRE. First launch Zookeeper and then Kafka, possibly set them up as services
so they relaunch on reboot.

Make sure that your Kafka server can accept connection from your apps. Especially the
`listeners` configuration value in your `server.properties` config file must be properly
set. If Kafka runs on the same machine as the apps, the default config should work.

If you are using Minio as a substitute of AWS S3, you may install it following its
[documentation](https://docs.minio.io/docs/minio-quickstart-guide.html). In such case,
make sure you set the environment variable S3_ENDPOINT_URL pointing to your local Minio
installation, by default "127.0.0.1:9000""

##### Python

For every app (the upload service and the consumer) create a virtual environment and
install its dependencies. Do this once in the upload-service root folder and once in
the `docker/consumer` folder.

    virtualenv . -p "$(which python3)"
    source bin/activate
    pip install -r requirements.txt

#### Running

Activate your Python virtual environment and run the upload service app pointing it
to your Kafka server by `KAFKAMQ` environment variable. For a local instance with
default settings this would be `KAFKAMQ=localhost:9092`.

    source bin/activate
    KAFKAMQ=localhost:9092 python app.py

    cd docker/consumer
    source bin/activate
    KAFKAMQ=localhost:9092 python app.py

## File upload

The upload service expects an `x-rh-identity` header, as this is provided
by 3scale when used in production. If you are running locally, you can
apply this header yourself. Below is a **base64** encoded string that
contains the following info:

    '{"identity": {"account_number": "12345", "internal": {"org_id": "54321"}}}'

Base64 String:

    eyJpZGVudGl0eSI6IHsiYWNjb3VudF9udW1iZXIiOiAiMTIzNDUiLCAiaW50ZXJuYWwiOiB7Im9yZ19pZCI6ICI1NDMyMSJ9fX0=

Upload a file to see if the system is working properly. Any file will work in testing
as long as the `type` field is set properly. Use the `README.md` file in this repo if
you'd like.

    curl -vvvv -H "x-rh-identity: <your base64 string>" -F "upload=@test-archive.tar.gz;type=application/vnd.redhat.testareno.something+tgz" -H "x-rh-insights-request-id: 52df9f748eabcfea" localhost:8080/api/ingress/v1/upload

If you’re running the upload service app directly and not in Docker, use port 8888 instead
of 8080 in the aforementioned command.

**NOTE**: The service **testareno** is important for local testing as it's the service queue
that our test consumer is listening to.

You should see messages in the docker logs where the upload-service sends a message,
the consumer picks it up, returns a failure message, then the upload-service sends it to
the permanent bucket.

For debugging purposes it’s also possible to produce/consume Kafka messages with its
own CLI tools. Run those using the following commands if you’re using Docker:

    sudo docker-compose exec kafka kafka-console-consumer --topic=testareno --bootstrap-server=localhost:29092
    sudo docker-compose exec kafka kafka-console-producer --topic=testareno --broker-list=localhost:29092

Otherwise if you’re running on bare metal, use these commands:

    kafka-console-consumer --topic=testareno --bootstrap-server=localhost:9092
    kafka-console-producer --topic=testareno --broker-list=localhost:9092

To see the docker-compose logs:

    sudo docker-compose logs -f

When running on bare metal, you’ll see the logs in your respective terminal windows with
the running apps.


## Running with Tests

Any new features added to the application should be accompanied by a Unittest/Pytest in `./tests`

To test, you'll need a python virtualenv with python3 and to install requirements:

    virtualenv . -p "$(which python3)"
    source bin/activate
    pip3 install -r requirements.txt

To test the app, activate the virtualenv and then run pytest and flake8.

    source bin/activate
    pytest ./tests
    flake8

There is several ways to generate the coverage report, but the commonly ways are:

    1. pytest --cov=.
    2. pytest --cov=. --cov-report html

**NOTE**: you will find the HTML report at `./htmlcov`

For last, but not less important, it is highly recommended to run all of your tests with `-rx` argument. There is a few tests that are using `pytest.xfail` which is a friendly way to flag that some test has failed, with this argument you'll be able to see the reason why those tests are failing.

e.g:

    pytest -rx --cov=.

For information on Tornado testing, see [the documentation](http://www.tornadoweb.org/en/stable/_modules/tornado/testing.html)

## Deployment

The upload service `master` branch has a webhook that notifies the Openshift Dedicated
cluster to build a new image. This image is immediately deployed in the **Platform-CI** project in the [insights-dev](https://console.insights-dev.openshift.com) cluster.
If this image is tested valid and operational, it should be tagged to QA for further testing. Once tested, it must be copied to the **platform-stage** project in the [production](https://console.insights.openshift.com) cluster. Once there and tested, it can be tagged to **platform-prod**.

The commands for that process are as follows:

    ===In insights-dev cluster===
    oc tag platform-ci/upload-service:latest platform-qa/upload-service:latest

    ===Copy to production cluster===
    skopeo copy --src-creds=user:dev_login_token --dest-creds=user:prod_login_token \
    docker://registry.insights-dev.openshift.com/platform-qa/upload-service:latest \
    docker://registry.insights.openshift.com/platform-stage/upload-service:latest

    ===In insights production cluster===
    oc tag platform-stage/upload-service:latest platform-prod/upload-service:latest

## Contributing

All outstanding issues or feature requests should be filed as Issues on this Github
page. PRs should be submitted against the master branch for any new features or changes.

## Versioning

Anytime an endpoint is modified, the versin should be incremented by `0.1`. New
functionality introduced that may effect the client should increment by `1`. Minor
features and bug fixes can increment by `0.0.1`

## Authors

* **Stephen Adams** - **Initial Work** - [SteveHNH](https://github.com/SteveHNH)
