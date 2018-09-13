pipeline {
  agent {
    node {
      label 'python3'
    }
  }
  environment {
    ASYNC_TEST_TIMEOUT=10
  }
  stages {
    stage('setup') {
      steps {
        echo "Setting up environment..."
        sh '/bin/python36 -m pip install --user -r requirements.txt'
      }
    }

    stage('code-check') {
      steps {
        echo "Checking code with flake8"
        sh '/bin/python36 -m flake8'
      }
    }

    stage('unit-tests') {
      steps {
        echo "Testing with pytest..."
        sh '/bin/python36 -m pytest -rxXs -s -v'
      }
    }
  }
}
