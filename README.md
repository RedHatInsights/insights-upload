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
    AWS credentials with S3 access

By default, the app will use the insights S3 account. If you do not have access
to this, you will need to provide your own AWS creds and buckets via environment
variables:

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

Once your environment variables are set on your localhost, bring up the stack:

    LINUX
    cd ./docker && sh startup.sh

    WINDOWS
    cd .\docker
    .\startup.ps1
    
This will stand up the full stack as well as initialize the topics in the message 
queue that are necessary for testing. The Kiel library does not automatically create 
topics in the MQ when they do not exist, so created them is critical.

### Bare metal

It’s possible to run the apps manually and make them connect to an existing Kafka instance.

#### Prerequisities

    python3 (preferrably 3.6) with venv
    zookeeper
    kafka

##### Queue

You can either connect to a remote kafka server, or set up a local one. To spawn your own
Kafka server, simply install it using your favorite package manager. To run Kafka, you need
Zookeeper and JRE. First launch Zookeeper and then Kafka, possibly set them up as services
so they relaunch on reboot.

Make sure that your Kafka server can accept connection from your apps. Especially the
`listeners` configuration value in your `server.properties` config file must be properly
set. If Kafka runs on the same machine as the apps, the default config should work.

Create the needed topics (advisor, available, testareno, uploadvalidation) in Kafka either
manually by running following command for every topic, or use the `create_topics.py`
Python script with a `ZOOKEEPER` environment variable set. For a local instance with
default settings, this would be `ZOOKEEPER=localhost:2181`.

    kafka-topics --create --topic "$TOPIC" --partitions 1 --replication-factor 1 --if-not-exists --zookeeper "$ZOOKEEPER_HOSTNAME:$ZOOKEEPER_PORT"

    ZOOKEEPER=localhost:2181 python3 create_topics.py

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

Upload a file to see if the system is working properly. Any file will work in testing
as long as the `type` field is set properly. Use the `README.md` file in this repo if
you'd like.

    curl -vvvv -F "upload=@test-archive.tar.gz;type=application/vnd.redhat.testareno.something+tgz" localhost:8080/api/v1/upload

If you’re running the upload service app directly and not in Docker, use port 8888 instead
of 8080 in the aforementioned command.

**NOTE**: The service **testareno** is important for local testing as it's the service queue
that our test consumer is listening to.

You should see messages in the docker logs where the upload-service sends a message,
the consumer picks it up, returns a failure message, then the upload-service sends it to
the permanent bucket.

For debugging purposes it’s also possible to produce/consume Kafka messages with its own CLI tools.

    sudo docker-compose exec kafka kafka-console-consumer --topic=testareno --bootstrap-server=localhost:29092
    sudo docker-compose exec kafka kafka-console-producer --topic=testareno --broker-list=localhost:29092

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
