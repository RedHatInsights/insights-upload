# Insights Platform Upload Service

The upload service is a component of Insights Platform that allows for customers
to upload data to Red Hat. The service sits behind a 3Scale API gateway that
handles authentication and routing. It runs on Tornado 5 and Python 3.6.

The upload service has an interface into Insights S3 Buckets for the storage of
customer data. It also connects to the Insights Message Bus in order to notify
services of new and available payloads for processing.

The service runs in Openshift Dedicated.

## Running Locally
**WIP**: Currently the server will fall over locally because it does not have access
to the message queue when it attempts to connect start. Working on a way around
this particular problem

Clone this repository

    git clone git@github.com:RedHatInsights/insights-upload

Setup a virtual environmnet and install dependencies

    pipenv install
    pipenv shell

OR

    virtualenv .
    source bin/activate
    pip install -r requirements

Run `app.py` to launch a local version of the upload service.

    python app.py

## Development
Follow the above steps to get an environment setup for development. Any new
features added to the application should be accompanied by a Unittest in `./tests`

To test the app, activate the virtualenv and then run pytest.

    source bin/activate
    pytest

For information on Tornado testing, see [the documentation](http://www.tornadoweb.org/en/stable/_modules/tornado/testing.html)

